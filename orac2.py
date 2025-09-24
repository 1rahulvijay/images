import pyarrow.parquet as pq
import oracledb
import logging
import sys
import time
from typing import List, Dict, Optional, Tuple, Iterator, Callable
import multiprocessing as mp
import xxhash
import pandas as pd
import pytest
import uuid
from contextlib import contextmanager

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
            logger.info("Parquet column structure validated successfully.")
        except FileNotFoundError:
            logger.error(f"Parquet file not found: {self.parquet_file}")
            raise
        except Exception as e:
            logger.error(f"Parquet validation error: {e}")
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
        except Exception as e:
            logger.error(f"Error reading Parquet file: {e}")
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
        pool_timeout: int = 30
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
            logger.error(f"Connection acquisition error: {e}")
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
            logger.info(f"Connection pool created with max {self.parallel_processes} connections")
        except oracledb.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up connection pool and log metrics."""
        if self.connection_pool:
            try:
                self.connection_pool.close()
                logger.info("Connection pool closed")
            except oracledb.Error as e:
                logger.error(f"Error closing connection pool: {e}")
        self.end_time = time.time()
        self.log_performance_metrics()

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
                    logger.error(f"Validation column '{col}' not found in expected columns")
                    valid = False
                    break
                try:
                    if not rule(row[col_indices[col]]):
                        logger.debug(f"Row {row_idx} failed validation for column '{col}'")
                        valid = False
                        break
                except Exception as e:
                    logger.warning(f"Validation error for column '{col}' in row {row_idx}: {e}")
                    valid = False
                    break
            if valid:
                validated.append(row)

        skipped = original_rows - len(validated)
        with self.rows_skipped.get_lock():
            self.rows_skipped.value += skipped
        if skipped > 0:
            logger.info(f"Skipped {skipped} invalid rows in batch")
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
                logger.info(f"Deduplicated {deduped_count} rows using primary keys")
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
                logger.info(f"Deduplicated {deduped_count} rows using xxhash")
            return deduped

    def transform_batch(self, batch_data: List[Tuple]) -> List[Tuple]:
        """Apply custom transformation function to batch."""
        if not self.transform_fn or not batch_data:
            return batch_data
        try:
            return self.transform_fn(batch_data)
        except Exception as e:
            logger.error(f"Transformation error: {e}")
            raise

    def load_batch(self, batch_data: List[Tuple], process_id: int) -> bool:
        """Load a single batch into Oracle with retries."""
        if not batch_data:
            logger.info(f"Empty batch for process {process_id}")
            return True

        batch_data = self.validate_data(self.transform_batch(self.deduplicate_batch(batch_data)))
        if not batch_data:
            logger.info(f"No valid data to load for process {process_id} after preprocessing")
            return True

        for attempt in range(1, self.max_retries + 1):
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
                    logger.info(f"Batch {process_id} loaded {len(batch_data)} rows on attempt {attempt}")
                    return True

                except oracledb.DatabaseError as e:
                    error_obj, = e.args
                    if error_obj.code == 1:  # ORA-00001: unique constraint violated
                        logger.warning(f"Batch {process_id} attempt {attempt} hit unique constraint violation")
                        if attempt == self.max_retries:
                            logger.error(f"Max retries reached for batch {process_id} due to unique constraint")
                            return False
                        continue
                    logger.error(f"Batch {process_id} failed on attempt {attempt}: {error_obj.message}")
                    if attempt == self.max_retries:
                        logger.error(f"Max retries reached for batch {process_id}. Saving to failed_batch_{process_id}.csv")
                        try:
                            pd.DataFrame(batch_data, columns=self.expected_columns).to_csv(
                                f"failed_batch_{process_id}_{uuid.uuid4().hex[:8]}.csv", index=False
                            )
                        except Exception as save_e:
                            logger.error(f"Failed to save failed batch: {save_e}")
                        return False
                finally:
                    if cur:
                        cur.close()

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
                    logger.info(f"Deleted {affected_rows} rows where {self.condition}")
                conn.commit()
            except oracledb.DatabaseError as e:
                logger.error(f"Error preparing table: {e}")
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
        
        with self.batch_times._getvalue() as bt:
            bt.append(batch_time)
        logger.info(f"Batch {batch_idx + 1} processed in {batch_time:.2f} seconds{' (failed)' if not success else ''}")

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
            logger.error(f"ETL process failed: {e}")
            raise

    def log_performance_metrics(self):
        """Log performance metrics."""
        elapsed_time = self.end_time - self.start_time if self.end_time else time.time() - self.start_time
        avg_batch_time = sum(self.batch_times) / len(self.batch_times) if self.batch_times else 0
        throughput = self.total_rows_loaded.value / elapsed_time if elapsed_time > 0 else 0
        logger.info("Performance Metrics:")
        logger.info(f"Total rows loaded: {self.total_rows_loaded.value}")
        logger.info(f"Rows skipped due to validation: {self.rows_skipped.value}")
        logger.info(f"Average time per batch: {avg_batch_time:.2f} seconds")
        logger.info(f"Overall elapsed time: {elapsed_time:.2f} seconds")
        logger.info(f"Throughput: {throughput:.2f} rows/second")

# Example transformation function
def example_transform(batch: List[Tuple]) -> List[Tuple]:
    """Example transformation: convert second column to uppercase."""
    return [(row[0], row[1].upper() if isinstance(row[1], str) else row[1], row[2]) for row in batch]

# Example usage
if __name__ == "__main__":
    reader = ParquetReader(
        parquet_file="large_data.parquet",
        batch_size=100000,
        columns=["col1", "col2", "col3"]
    )
    
    with OracleLoader(
        table_name="MY_TABLE",
        connection_string="user/password@host:port/service_name",
        expected_columns=["col1", "col2", "col3"],
        primary_keys=["col1"],
        batch_size=100000,
        mode="append",
        condition="col1 > 100",
        validation_rules={"col2": lambda x: isinstance(x, str) and len(x) > 0},
        transform_fn=example_transform,
        max_retries=3,
        parallel_processes=4,
        pool_timeout=30
    ) as loader:
        loader.etl_process(reader)

# Unit Tests
def test_validate_columns():
    # Requires mocking ParquetFile for proper testing
    with pytest.raises(FileNotFoundError):
        reader = ParquetReader("nonexistent.parquet", 1000, ["a", "b"])
        reader.validate_columns()

def test_validate_data():
    loader = OracleLoader(
        table_name="dummy", connection_string="dummy", expected_columns=["age", "name"],
        validation_rules={"age": lambda x: x > 0}
    )
    data = [(5, "Alice"), (-1, "Bob"), (10, "Charlie")]
    validated = loader.validate_data(data)
    assert len(validated) == 2
    assert validated == [(5, "Alice"), (10, "Charlie")]

def test_deduplicate_batch():
    loader = OracleLoader(
        table_name="dummy", connection_string="dummy", expected_columns=["id", "name"],
        primary_keys=["id"]
    )
    data = [(1, "A"), (1, "A"), (2, "B")]
    deduped = loader.deduplicate_batch(data)
    assert len(deduped) == 2
    assert set(deduped) == {(1, "A"), (2, "B")}

def test_empty_batch():
    loader = OracleLoader(
        table_name="dummy", connection_string="dummy", expected_columns=["id", "name"]
    )
    assert loader.load_batch([], process_id=1) == True

def test_transform_batch():
    loader = OracleLoader(
        table_name="dummy", connection_string="dummy", expected_columns=["id", "name", "value"],
        transform_fn=example_transform
    )
    data = [(1, "alice", 100), (2, "bob", 200)]
    transformed = loader.transform_batch(data)
    assert transformed == [(1, "ALICE", 100), (2, "BOB", 200)]
