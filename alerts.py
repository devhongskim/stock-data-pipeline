import os
import logging
import requests

logger = logging.getLogger(__name__)


def send_failure_alert(context):
    """
    Airflow on_failure_callback. Fires automatically whenever a task in the DAG
    fails (including after retries are exhausted), posting the failure details
    to Slack via an Incoming Webhook.

    If SLACK_WEBHOOK_URL isn't configured, this degrades gracefully to a log
    line instead of raising -- a broken alerting integration should never be
    the reason a pipeline failure goes unnoticed OR compounds into a second failure.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    execution_date = context.get("logical_date") or context.get("execution_date")
    exception = context.get("exception")
    log_url = task_instance.log_url if task_instance else None

    message_lines = [
        "🚨 *Airflow Task Failed*",
        f"*DAG*: `{dag_id}`",
        f"*Task*: `{task_id}`",
        f"*Logical Date*: {execution_date}",
        f"*Error*: {exception}",
    ]
    if log_url:
        message_lines.append(f"*Logs*: {log_url}")
    message = "\n".join(message_lines)

    if not webhook_url:
        logger.warning(
            "SLACK_WEBHOOK_URL not set -- skipping Slack alert. Failure details:\n%s",
            message,
        )
        return

    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=10)
        response.raise_for_status()
        logger.info("Slack failure alert sent successfully.")
    except Exception as e:
        # Never let a broken alert crash the failure-handling path itself
        logger.error(f"Failed to send Slack failure alert: {e}")