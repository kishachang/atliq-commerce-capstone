"""
AtliQ Phase 2 (LEARNER STARTER) — Streaming Ops DAG
====================================================
The stream never stops — but the SCHEDULED work around it is your job:
data-quality gate, table maintenance, daily rollup. Build an hourly DAG:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml:  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://<workspace>.cloud.databricks.com | Password: <PAT token>
3. Paste your SQL warehouse HTTP path below.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/c1a027589ea7c6d2"   # <-- yours

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["dbw_atliq_capstone", "phase2", "streaming"],
) as dag:

    # TODO 1 — Data-quality gate.
    # A DatabricksSqlOperator that FAILS when no events landed recently.
    # Hint: Databricks SQL has assert_true(condition, message) — write a SELECT
    # that asserts COUNT(*) > 0 over silver_order_events for the last 2 hours.
    # A DQ check that can never fail is worth zero marks — you must be able to
    # demo it failing when the producer is stopped.
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            SELECT assert_true(
                COUNT(*) > 0,
                'DQ FAILURE: No streaming events received in the last 2 hours'
            )
            FROM dbw_atliq_capstone.streaming.silver_order_events
            WHERE event_ts >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
        """,
    )

    # TODO 2 — Table maintenance.
    # Streaming writes create many small files. Run OPTIMIZE on
    # silver_order_events and gold_revenue_5min.
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            "OPTIMIZE dbw_atliq_capstone.streaming.silver_order_events",
            "OPTIMIZE dbw_atliq_capstone.streaming.gold_revenue_5min",
        ],
    )

    # TODO 3 — Daily rollup.
    # CREATE OR REPLACE atliq.streaming.gold_daily_summary: per event_date —
    # orders_placed, orders_paid, orders_cancelled, revenue (from paid events).
    # Hint: COUNT_IF() and a CASE inside SUM().
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            CREATE OR REPLACE TABLE dbw_atliq_capstone.streaming.gold_daily_summary AS

            SELECT
                DATE(event_ts) AS event_date,

                COUNT_IF(
                    event_type = 'order_placed'
                ) AS orders_placed,

                COUNT_IF(
                    event_type = 'payment_received'
                ) AS orders_paid,

                COUNT_IF(
                    event_type = 'order_cancelled'
                ) AS orders_cancelled,

                SUM(
                    CASE
                        WHEN event_type = 'payment_received'
                        THEN order_amount
                        ELSE 0
                    END
                ) AS revenue

            FROM dbw_atliq_capstone.streaming.silver_order_events

            GROUP BY DATE(event_ts)
        """,
    )

    # TODO 4 — Chain them in order:
    check_fresh_events >> optimize_tables >> refresh_daily_summary
