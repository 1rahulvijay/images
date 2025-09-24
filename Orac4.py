import pyarrow.parquet as pq
import oracledb
import logging
import sys
import time
import yaml
import json
from typing import List, Dict, Optional, Tuple, Iterator, Callable
import multiprocessing as mp
import xxhash
import pandas as pd
import pytest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
import tenacity  # For retry logic

# For Airflow integration
try:
    from airflow.decorators import dag, task
    from airflow.operators.python import PythonOperator
    from airflow.models import Variable
    from airflow.utils.task_group import TaskGroup
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    logging.warning("Airflow not available; DAG will not be defined.")

# Configure logging with file handler for robustness
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler('etl_log.txt', mode='a')]
)
logger = logging.getLogger(__name__)

class ConfigLoader:
    """Handles loading and validating configuration from YAML file or dictionary."""
    
    @staticmethod
    def load_config(config_path: Optional[str] = None, config_dict: Optional[List[Dict]] = None) -> List[Dict]:
        """Load configuration from YAML file or list of dictionaries. Returns list for multi-config support."""
        try:
            if config_path:
                with open(config_path, 'r') as f:
                    data = yaml.safe_load(f)
                return data if isinstance(data, list) else [data]
            elif config_dict:
                return config_dict if isinstance(config_dict, list) else [config_dict]
            else:
                raise ValueError("Either config_path or config_dict must be provided")
        except Exception as e:
            logger.error(f"Config loading failed: {e}")
            raise
    
    @staticmethod
    def validate_config(configs: List[Dict]) -> List[Dict]:
        """Validate and return list of configurations."""
        validated = []
        for idx, conf in enumerate(configs):
            required_keys = ['parquet_file', 'table_name', 'connection_string', 'expected_columns']
            missing = [key for key in required_keys if key not in conf]
            if missing:
                raise ValueError(f"Missing required keys {missing} in config {idx+1}")
            if 'primary_keys' in conf and not isinstance(conf['primary_keys'], list):
                raise ValueError(f"primary_keys must be a list in config {idx+1}")
            if 'parallel_processes' in conf:
                conf['parallel_processes'] = min(max(1, conf['parallel_processes']), 4)
            conf['id'] = conf.get('id', f"config_{idx+1}")  # Unique ID for status tracking
            validated.append(conf)
        return validated

class StatusTracker:
    """Handles tracking load status for multiple configs using a JSON file."""
    
    def __init__(self, status_file: str = "load_status.json"):
        self.status_file = Path(status_file)
        self.status = self._load_status()
    
    def _load_status(self) -> Dict[str, Dict]:
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in status file {self.status_file}: {e}")
                return {}
        return {}
    
    def _save_status(self):
        try:
            with open(self.status_file, 'w') as f:
                json.dump(self.status, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save status file {self.status_file}: {e}")
    
    def is_succeeded(self, config_id: str) -> bool:
        return self.status.get(config_id, {}).get('status') == 'succeeded'
    
    def update_status(self, config_id: str, status: str, message: Optional[str] = None):
        self.status[config_id] = {'status': status, 'message': message, 'timestamp': datetime.now().isoformat()}
        self._save_status()

class ParquetReader:
    """Handles reading and batching of Parquet file data."""
    
    def __init__(self, parquet_file: str, batch_size: int, columns: List[str]):
        if not columns:
            raise ValueError("Columns list cannot be empty")
        self.parquet_file = parquet_file
        self.batch_size = batch_size
        self.columns = columns
        
    def validate_columns(self) -> None:
        """Validate Parquet file column structure."""
        try:
            parquet_file = pq.ParquetFile(self.parquet_file)
            schema_columns = [field.name for field in parquet_file.schema_arrow]
            if set(schema_columns) != set(self.columns):
                raise ValueError(f"Column mismatch. Expected: {self.columns}, Got: {schema_columns}")
            logger.info(f"Parquet column structure validated successfully for file {self.parquet_file}.")
        except FileNotFoundError:
            logger.error(f"Parquet file not found: {self.parquet_file}")
            raise
        except Exception as e:
            logger.error(f"Parquet validation error for {self.parquet_file}: {e}")
            raise

    def get_batches(self) -> Iterator[Tuple[int, List[Tuple]]]:
        """Yields batches of data as tuples with batch index."""
        try:
            parquet_file = pq.ParquetFile(self.parquet_file)
            for i, arrow_batch in enumerate(parquet_file.iter_batches(
                batch_size=self.batch_size,
                columns=self.columns
            )):
                batch_data = list(zip(*[arrow_batch.column(col).to_pylist() for col in self.columns]))
                yield i, batch_data
                logger.info(f"Read batch {i+1} from {self.parquet_file} ({len(batch_data)} rows)")
        except Exception as e:
            logger.error(f"Error reading Parquet file {self.parquet_file}: {e}")
            raise

class OracleLoader:
    """Handles loading data into Oracle database with parallel processing."""
    
    VALID_MODES = {'append', 'replace', 'append_with_condition', 'delete_with_condition'}
    
    def __init__(
        self,
        table_name: str,
        connection_string: str,
        expected_columns: List[str],
        primary_keys: List[str] = None,
        batch_size: int = 100000,
        mode: str = 'append',
        condition: Optional[str] = None,
        validation_rules: Optional[Dict[str, Callable]] = None,
        transform_fn: Optional[Callable[[List[Tuple]], List[Tuple]]] = None,
        max_retries: int = 3,
        parallel_processes: int = 4,
        pool_timeout: int = 30,
        post_load_check: Optional[Callable[[str, str], bool]] = None
    ):
        if not table_name or not connection_string or not expected_columns:
            raise ValueError("table_name, connection_string, and expected_columns must be provided")
        if mode.lower() not in self.VALID_MODES:
            raise ValueError(f"Invalid mode: {mode}. Supported: {self.VALID_MODES}")
        if primary_keys and not all(key in expected_columns for key in primary_keys):
            raise ValueError("All primary keys must be in expected_columns")
            
        self.table_name = table_name
        self.connection_string = connection_string
        self.expected_columns = expected_columns
        self.primary_keys = primary_keys or []
        self.batch_size = max(1, batch_size)
        self.mode = mode.lower()
        self.condition = condition
        self.validation_rules = validation_rules or {}
        self.transform_fn = transform_fn
        self.max_retries = max(1, max_retries)
        self.parallel_processes = min(max(1, parallel_processes), 4)
        self.pool_timeout = pool_timeout
        self.post_load_check = post_load_check
        self.connection_pool = None
        
        # Performance metrics
        self.total_rows_loaded = mp.Value('i', 0)
        self.rows_skipped = mp.Value('i', 0)
        self.batch_times = mp.Manager().list()
        self.start_time = None
        self.end_time = None

    @contextmanager
    def connection(self):
        """Context manager for acquiring and releasing a connection from the pool."""
        conn = None
        try:
            conn = self.connection_pool.acquire()
            yield conn
        except oracledb.Error as e:
            logger.error(f"Connection acquisition error for table {self.table_name}: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.release(conn)

    def __enter__(self):
        """Initialize connection pool."""
        self.start_time = time.time()
        try:
            self.connection_pool = oracledb.create_pool(
                dsn=self.connection_string,
                min=1,
                max=self.parallel_processes,
                increment=1,
                getmode=oracledb.POOL_GETMODE_WAIT,
                timeout=self.pool_timeout
            )
            logger.info(f"Connection pool created with max {self.parallel_processes} connections for table {self.table_name}")
        except oracledb.Error as e:
            logger.error(f"Failed to create connection pool for table {self.table_name}: {e}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up connection pool and log metrics."""
        if self.connection_pool:
            try:
                self.connection_pool.close()
                logger.info(f"Connection pool closed for table {self.table_name}")
            except oracledb.Error as e:
                logger.error(f"Error closing connection pool for table {self.table_name}: {e}")
        self.end_time = time.time()
        self.log_performance_metrics()
        if self.post_load_check:
            try:
                self.post_load_check(self.table_name, self.connection_string)
            except Exception as e:
                logger.warning(f"Post-load check failed for table {self.table_name}: {e}")

    def validate_data(self, batch_data: List[Tuple]) -> List[Tuple]:
        """Apply data validation rules to batch."""
        if not self.validation_rules or not batch_data:
            return batch_data

        original_rows = len(batch_data)
        col_indices = {col: idx for idx, col in enumerate(self.expected_columns)}
        validated = []
        
        for row_idx, row in enumerate(batch_data):
            valid = True
            for col, rule in self.validation_rules.items():
                if col not in col_indices:
                    logger.error(f"Validation column '{col}' not found in expected columns for table {self.table_name}")
                    valid = False
                    break
                try:
                    if not rule(row[col_indices[col]]):
                        logger.debug(f"Row {row_idx} failed validation for column '{col}' in table {self.table_name}")
                        valid = False
                        break
                except Exception as e:
                    logger.warning(f"Validation error for column '{col}' in row {row_idx} for table {self.table_name}: {e}")
                    valid = False
                    break
            if valid:
                validated.append(row)

        skipped = original_rows - len(validated)
        with self.rows_skipped.get_lock():
            self.rows_skipped.value += skipped
        if skipped > 0:
            logger.info(f"Skipped {skipped} invalid rows in batch for table {self.table_name}")
        return validated

    def deduplicate_batch(self, batch_data: List[Tuple]) -> List[Tuple]:
        """Deduplicate within batch using primary keys or row hash."""
        if not batch_data:
            return []

        if self.primary_keys:
            key_indices = [self.expected_columns.index(key) for key in self.primary_keys]
            seen = {}
            deduped = []
            for row in batch_data:
                key = tuple(row[i] for i in key_indices)
                if key not in seen:
                    seen[key] = row
                    deduped.append(row)
            deduped_count = len(batch_data) - len(deduped)
            if deduped_count > 0:
                logger.info(f"Deduplicated {deduped_count} rows using primary keys for table {self.table_name}")
            return deduped
        else:
            hasher = xxhash.xxh64()
            seen = {}
            deduped = []
            for row in batch_data:
                hasher.update(str(row).encode())
                row_hash = hasher.hexdigest()
                if row_hash not in seen:
                    seen[row_hash] = row
                    deduped.append(row)
            deduped_count = len(batch_data) - len(deduped)
            if deduped_count > 0:
                logger.info(f"Deduplicated {deduped_count} rows using xxhash for table {self.table_name}")
            return deduped

    def transform_batch(self, batch_data: List[Tuple]) -> List[Tuple]:
        """Apply custom transformation function to batch."""
        if not self.transform_fn or not batch_data:
            return batch_data
        try:
            return self.transform_fn(batch_data)
        except Exception as e:
            logger.error(f"Transformation error for table {self.table_name}: {e}")
            raise

    def load_batch(self, batch_data: List[Tuple], process_id: int) -> bool:
        """Load a single batch into Oracle with retries."""
        if not batch_data:
            logger.info(f"Empty batch for process {process_id} in table {self.table_name}")
            return True

        batch_data = self.validate_data(self.transform_batch(self.deduplicate_batch(batch_data)))
        if not batch_data:
            logger.info(f"No valid data to load for process {process_id} after preprocessing in table {self.table_name}")
            return True

        @tenacity.retry(stop=tenacity.stop_after_attempt(self.max_retries), wait=tenacity.wait_exponential(min=1, max=10), reraise=True)
        def attempt_load():
            with self.connection() as conn:
                cur = None
                try:
                    cur = conn.cursor()
                    cur.arraysize = self.batch_size

                    if self.primary_keys and self.mode in ['append', 'append_with_condition']:
                        on_condition = ' AND '.join([f"t.{key} = s.{key}" for key in self.primary_keys])
                        columns = ', '.join(self.expected_columns)
                        placeholders = ', '.join([f":{col}" for col in self.expected_columns])
                        update_cols = [col for col in self.expected_columns if col not in self.primary_keys]
                        update_clause = ', '.join([f"t.{col} = s.{col}" for col in update_cols]) or "t.dummy = s.dummy"
                        merge_sql = f"""
                        MERGE INTO {self.table_name} t
                        USING (SELECT {placeholders}, 1 dummy FROM dual) s ({columns}, dummy)
                        ON ({on_condition})
                        WHEN MATCHED THEN UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({columns})
                        """
                        data = [dict(zip(self.expected_columns + ['dummy'], row + (1,))) for row in batch_data]
                        cur.executemany(merge_sql, data)
                    else:
                        columns = ', '.join(self.expected_columns)
                        placeholders = ', '.join([f":{i+1}" for i in range(len(self.expected_columns))])
                        insert_sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
                        cur.executemany(insert_sql, batch_data)

                    conn.commit()
                    with self.total_rows_loaded.get_lock():
                        self.total_rows_loaded.value += len(batch_data)
                    logger.info(f"Batch {process_id} loaded {len(batch_data)} rows for table {self.table_name}")
                except oracledb.DatabaseError as e:
                    error_obj, = e.args
                    logger.error(f"Batch {process_id} failed for table {self.table_name}: {error_obj.message}")
                    conn.rollback()
                    raise
                finally:
                    if cur:
                        cur.close()

        try:
            attempt_load()
            return True
        except tenacity.RetryError as e:
            logger.error(f"Max retries reached for batch {process_id} in table {self.table_name}. Saving failed batch.")
            try:
                pd.DataFrame(batch_data, columns=self.expected_columns).to_csv(
                    f"failed_batch_{self.table_name}_{process_id}_{uuid.uuid4().hex[:8]}.csv", index=False
                )
            except Exception as save_e:
                logger.error(f"Failed to save batch for table {self.table_name}: {save_e}")
            return False

    def prepare_table(self):
        """Prepare table based on mode."""
        with self.connection() as conn:
            cur = None
            try:
                cur = conn.cursor()
                if self.mode == 'replace':
                    cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    logger.info(f"Table {self.table_name} truncated for replace mode")
                elif self.mode == 'delete_with_condition' and self.condition:
                    cur.execute(f"DELETE FROM {self.table_name} WHERE {self.condition}")
                    affected_rows = cur.rowcount
                    logger.info(f"Deleted {affected_rows} rows where {self.condition} for table {self.table_name}")
                conn.commit()
            except oracledb.DatabaseError as e:
                logger.error(f"Error preparing table {self.table_name}: {e}")
                conn.rollback()
                raise
            finally:
                if cur:
                    cur.close()

    def process_batch(self, batch_data: Tuple[int, List[Tuple]]):
        """Process a single batch for multiprocessing."""
        batch_idx, batch = batch_data
        batch_start = time.time()
        
        success = self.load_batch(batch, process_id=batch_idx + 1)
        batch_time = time.time() - batch_start
        
        with self.batch_times.get_lock():
            self.batch_times.append(batch_time)
        logger.info(f"Batch {batch_idx + 1} processed in {batch_time:.2f} seconds for table {self.table_name} {' (failed)' if not success else ''}")

    def etl_process(self, reader: ParquetReader):
        """Execute ETL process with parallel batch loading."""
        try:
            reader.validate_columns()
            self.prepare_table()
            
            if self.parallel_processes > 1:
                with mp.Pool(processes=self.parallel_processes, maxtasksperchild=10) as pool:
                    pool.map(self.process_batch, reader.get_batches())
            else:
                for batch in reader.get_batches():
                    self.process_batch(batch)
                    
        except Exception as e:
            logger.error(f"ETL process failed for table {self.table_name}: {e}")
            raise

    def log_performance_metrics(self):
        """Log performance metrics."""
        elapsed_time = self.end_time - self.start_time if self.end_time else time.time() - self.start_time
        avg_batch_time = sum(self.batch_times) / len(self.batch_times) if self.batch_times else 0
        throughput = self.total_rows_loaded.value / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"Performance Metrics for table {self.table_name}:")
        logger.info(f"Total rows loaded: {self.total_rows_loaded.value}")
        logger.info(f"Rows skipped due to validation: {self.rows_skipped.value}")
        logger.info(f"Average time per batch: {avg_batch_time:.2f} seconds")
        logger.info(f"Overall elapsed time: {elapsed_time:.2f} seconds")
        logger.info(f"Throughput: {throughput:.2f} rows/second")

# Example transformation function
def example_transform(batch: List[Tuple]) -> List[Tuple]:
    """Example transformation: convert second column to uppercase."""
    return [(row[0], row[1].upper() if isinstance(row[1], str) else row[1], row[2]) for row in batch]

# Example post-load check
def example_post_check(table_name: str, connection_string: str) -> bool:
    """Example post-load check: verify row count > 0."""
    with oracledb.connect(connection_string) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        cur.close()
        logger.info(f"Post-load check: {count} rows in {table_name}")
        return count > 0

# Standalone usage example
if __name__ == "__main__":
    # Example: Load configs from JSON file
    configs = ConfigLoader.load_config(config_path="multi_config.json")
    configs = ConfigLoader.validate_config(configs)
    run_multi_etl(configs, status_file="load_status.json", force_reload=False)

# Airflow DAG (if Airflow is available)
if AIRFLOW_AVAILABLE:
    @dag(
        dag_id='multi_oracle_etl_dag',
        default_args={
            'owner': 'data-team',
            'depends_on_past': False,
            'start_date': datetime(2025, 9, 24),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        },
        description='ETL multiple Parquet files to Oracle tables, with independent task failures',
        schedule_interval='@daily',
        catchup=False,
        tags=['etl', 'oracle', 'multi-table'],
        max_active_runs=1,
    )
    def multi_oracle_etl_dag():
        @task
        def load_configs() -> List[Dict]:
            """Load list of configurations from Airflow Variable (JSON list)."""
            configs_str = Variable.get("multi_oracle_etl_configs", default_var="[]")
            configs = json.loads(configs_str)
            return ConfigLoader.validate_config(configs)

        @task
        def run_etl_for_config(conf: Dict):
            """Run ETL for a single config."""
            status_file = Variable.get("status_file", default_var="load_status.json")
            tracker = StatusTracker(status_file)
            config_id = conf['id']
            if tracker.is_succeeded(config_id):
                logger.info(f"Skipping succeeded load for config {config_id} (table {conf['table_name']})")
                return f"Skipped for {conf['table_name']}"
            try:
                reader = ParquetReader(
                    parquet_file=conf['parquet_file'],
                    batch_size=conf.get('batch_size', 100000),
                    columns=conf['expected_columns']
                )
                with OracleLoader(
                    table_name=conf['table_name'],
                    connection_string=Variable.get("oracle_connection_string"),
                    expected_columns=conf['expected_columns'],
                    primary_keys=conf.get('primary_keys'),
                    batch_size=conf.get('batch_size', 100000),
                    mode=conf.get('mode', 'append'),
                    condition=conf.get('condition'),
                    validation_rules=conf.get('validation_rules', {}),
                    transform_fn=conf.get('transform_fn'),
                    max_retries=conf.get('max_retries', 3),
                    parallel_processes=conf.get('parallel_processes', 4),
                    pool_timeout=conf.get('pool_timeout', 30),
                    post_load_check=conf.get('post_load_check')
                ) as loader:
                    loader.etl_process(reader)
                tracker.update_status(config_id, 'succeeded')
                return f"Success for {conf['table_name']}"
            except Exception as e:
                tracker.update_status(config_id, 'failed', str(e))
                raise ValueError(f"Failed for {conf['table_name']}: {e}")

        configs = load_configs()
        with TaskGroup(group_id="load_tasks") as tg:
            for conf in configs:  # Dynamic tasks based on configs
                run_etl_for_config.override(task_id=f"load_{conf['id']}")(conf)

        configs >> tg

    # Instantiate the DAG
    multi_oracle_etl_dag_instance = multi_oracle_etl_dag()

# Unit Tests (expanded)
def test_config_loader():
    config = [{'parquet_file': 'test.parquet', 'table_name': 'test', 'connection_string': 'test', 'expected_columns': ['col1']}]
    validated = ConfigLoader.validate_config(config)
    assert len(validated) == 1
    assert validated[0]['id'] == 'config_1'

def test_status_tracker():
    tracker = StatusTracker('test_status.json')
    tracker.update_status('test_id', 'succeeded')
    assert tracker.is_succeeded('test_id')
    Path('test_status.json').unlink()  # Cleanup

def test_validate_data():
    loader = OracleLoader(table_name="dummy", connection_string="dummy", expected_columns=["age"], validation_rules={"age": lambda x: x > 0})
    data = [(5,), (-1,), (10,)]
    validated = loader.validate_data(data)
    assert len(validated) == 2

def test_deduplicate_batch():
    loader = OracleLoader(table_name="dummy", connection_string="dummy", expected_columns=["id", "name"], primary_keys=["id"])
    data = [(1, "A"), (1, "A"), (2, "B")]
    deduped = loader.deduplicate_batch(data)
    assert len(deduped) == 2
