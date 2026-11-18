"""Run notifications: Amazon SNS when ALERT_SNS_TOPIC_ARN is configured, task log otherwise."""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def _publish(subject: str, message: str) -> None:
    topic_arn = os.getenv("ALERT_SNS_TOPIC_ARN")
    if not topic_arn:
        log.warning("[alert] %s\n%s", subject, message)
        return
    from airflow.providers.amazon.aws.hooks.sns import SnsHook

    SnsHook(aws_conn_id=os.getenv("AWS_CONN_ID", "aws_default")).publish_to_target(
        target_arn=topic_arn, subject=subject[:100], message=message
    )


def notify_failure(context: dict) -> None:
    ti = context["task_instance"]
    _publish(
        f"[FAILED] {ti.dag_id}.{ti.task_id}",
        "\n".join(
            [
                f"DAG run:   {context['run_id']}",
                f"Task:      {ti.task_id} (try {ti.try_number})",
                f"Exception: {context.get('exception')}",
                f"Log:       {ti.log_url}",
            ]
        ),
    )


def notify_summary(subject: str, message: str) -> None:
    _publish(subject, message)
