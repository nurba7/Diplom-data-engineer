from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='full_pipeline_master',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_sales = TriggerDagRunOperator(
        task_id="trigger_load_sales_data_dag",
        trigger_dag_id="load_sales_data_dag",
    )

    load_nds = TriggerDagRunOperator(
        task_id="trigger_etl_sales_to_nds",
        trigger_dag_id="etl_sales_to_nds",
    )

    load_dds = TriggerDagRunOperator(
        task_id="trigger_etl_nds_to_dds",
        trigger_dag_id="etl_nds_to_dds",
    )

    load_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality_check_dag",
        trigger_dag_id="data_quality_check_dag",
    )

    load_sales >> load_nds >> load_dds >> load_quality
