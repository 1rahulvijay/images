# plugins/advanced_robust_operators.py
import os
import logging
import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException
from airflow.utils.email import send_email
from airflow.operators.python import PythonOperator as AirflowPythonOperator
from airflow.operators.python import PythonVirtualenvOperator
import papermill as pm


class AdvancedPapermillOperator(BaseOperator):
    """
    Execute Jupyter notebooks with Papermill with advanced features:
      - Automatic retries with exponential backoff
      - Kernel selection
      - XCom push of output notebook path
      - Custom email notifications with optional attachment
      - Automatic timestamped output path
      - Execution metadata logging
      - Custom messages
    """

    @apply_defaults
    def __init__(
        self,
        input_path: str,
        output_dir: str,
        parameters: dict = None,
        kernel_name: str = None,
        email: list[str] = None,
        attach_output_in_email: bool = False,
        custom_message: str = "",
        *args,
        **kwargs
    ):
        super().__init__(
            *args,
            email_on_failure=bool(email),
            retries=kwargs.get("retries", 3),
            retry_exponential_backoff=True,
            retry_delay=kwargs.get("retry_delay", datetime.timedelta(seconds=10)),
            **kwargs
        )
        self.input_path = input_path
        self.output_dir = output_dir
        self.parameters = parameters or {}
        self.kernel_name = kernel_name
        self.email = email
        self.attach_output_in_email = attach_output_in_email
        self.custom_message = custom_message

    def _send_email(self, subject: str, html_content: str, attachment_path: str = None):
        if self.email:
            try:
                send_email(to=self.email, subject=subject, html_content=html_content, files=[attachment_path] if attachment_path else None)
                logging.info("Custom email sent successfully")
            except Exception as e:
                logging.warning(f"Failed to send email: {e}")

    def execute(self, context):
        start_time = datetime.datetime.now()
        task_id = context['task_instance'].task_id

        if not os.path.exists(self.input_path):
            raise AirflowFailException(f"Input notebook does not exist: {self.input_path}")

        # Generate timestamped output path
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        notebook_name = os.path.basename(self.input_path).replace(".ipynb", "")
        output_path = os.path.join(self.output_dir, f"{notebook_name}_{timestamp}.ipynb")

        try:
            logging.info(f"Executing notebook: {self.input_path} with kernel: {self.kernel_name}")
            pm.execute_notebook(
                input_path=self.input_path,
                output_path=output_path,
                parameters=self.parameters,
                kernel_name=self.kernel_name,
                log_output=True
            )

            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Notebook executed successfully: {output_path}")

            # Push to XCom
            context['ti'].xcom_push(key="output_notebook_path", value=output_path)

            # Send success email
            subject = f"[Airflow] Task Success: {task_id}"
            html_content = f"""
                <p>Task <b>{task_id}</b> executed successfully.</p>
                <p>Execution time: {duration:.2f} seconds</p>
                <p>Output notebook: {output_path}</p>
                <p>{self.custom_message}</p>
            """
            if self.attach_output_in_email:
                self._send_email(subject, html_content, attachment_path=output_path)
            else:
                self._send_email(subject, html_content)

            return output_path

        except Exception as e:
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Send failure email
            subject = f"[Airflow] Task Failed: {task_id}"
            html_content = f"""
                <p>Task <b>{task_id}</b> failed.</p>
                <p>Execution time: {duration:.2f} seconds</p>
                <p>Error: {e}</p>
                <p>{self.custom_message}</p>
            """
            self._send_email(subject, html_content)
            raise AirflowFailException(f"Notebook execution failed: {e}")


class AdvancedPythonOperator(PythonVirtualenvOperator):
    """
    Python operator with advanced features:
      - Runs in isolated virtualenv (kernel-like isolation)
      - Retries with exponential backoff
      - Custom success/failure email notifications
      - XCom push of return value
      - Execution metadata logging
      - Custom messages
    """

    @apply_defaults
    def __init__(
        self,
        python_callable,
        requirements: list[str] = None,
        email: list[str] = None,
        custom_message: str = "",
        *args,
        **kwargs
    ):
        super().__init__(
            python_callable=python_callable,
            requirements=requirements or [],
            system_site_packages=False,
            retries=kwargs.get("retries", 3),
            retry_exponential_backoff=True,
            retry_delay=kwargs.get("retry_delay", datetime.timedelta(seconds=10)),
            *args,
            **kwargs
        )
        self.email = email
        self.custom_message = custom_message

    def _send_email(self, subject: str, html_content: str):
        if self.email:
            try:
                send_email(to=self.email, subject=subject, html_content=html_content)
                logging.info("Custom email sent successfully")
            except Exception as e:
                logging.warning(f"Failed to send email: {e}")

    def execute(self, context):
        task_id = context['task_instance'].task_id
        start_time = datetime.datetime.now()

        try:
            result = super().execute(context)
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Push result to XCom
            context['ti'].xcom_push(key="python_task_result", value=result)

            # Success email
            subject = f"[Airflow] Task Success: {task_id}"
            html_content = f"""
                <p>Task <b>{task_id}</b> executed successfully.</p>
                <p>Execution time: {duration:.2f} seconds</p>
                <p>{self.custom_message}</p>
            """
            self._send_email(subject, html_content)

            return result

        except Exception as e:
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Failure email
            subject = f"[Airflow] Task Failed: {task_id}"
            html_content = f"""
                <p>Task <b>{task_id}</b> failed.</p>
                <p>Execution time: {duration:.2f} seconds</p>
                <p>Error: {e}</p>
                <p>{self.custom_message}</p>
            """
            self._send_email(subject, html_content)
            raise
