from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def run_data_quality_checks():
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='123',
        host='host.docker.internal',
        port='5432'
    )
    cur = conn.cursor()
     
    def log_quality_check(name, description, result, row_count):
        # Создание таблицы 
        cur.execute("""
        CREATE TABLE if not exists nds.data_quality_log (
            check_id SERIAL PRIMARY KEY,
            check_name VARCHAR(100),
            check_description TEXT,
            check_result VARCHAR(20),
            row_count INTEGER,
            check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
        conn.commit()
        
        # Заргузка резульататов
        cur.execute("""
            INSERT INTO nds.data_quality_log (check_name, check_description, check_result, row_count)
            VALUES (%s, %s, %s, %s)
        """, (name, description, result, row_count))
        conn.commit()

    # Проверка на null по invoice_id
    cur.execute("SELECT COUNT(*) FROM nds.nds_sales WHERE invoice_id IS NULL")
    nulls = cur.fetchone()[0]
    log_quality_check(
        name="Check NULL invoice_id",
        description="Проверка на NULL в nds_sales.invoice_id",
        result="FAIL" if nulls > 0 else "PASS",
        row_count=nulls
    )

    # Проверка на дубликаты invoice_id
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT invoice_id FROM nds.nds_sales
            GROUP BY invoice_id HAVING COUNT(*) > 1
        ) AS duplicates
    """)
    dupes = cur.fetchone()[0]
    log_quality_check(
        name="Check Duplicate invoice_id",
        description="Проверка на дубли invoice_id в nds_sales",
        result="FAIL" if dupes > 0 else "PASS",
        row_count=dupes
    )

    # Проверка на отрицательные значения total
    cur.execute("SELECT COUNT(*) FROM nds.nds_sales WHERE total < 0")
    negatives = cur.fetchone()[0]
    log_quality_check(
        name="Check Negative Total",
        description="Проверка на отрицательные значения total в nds_sales",
        result="FAIL" if negatives > 0 else "PASS",
        row_count=negatives
    )

    conn.close()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='data_quality_check_dag',
    default_args=default_args,
    schedule_interval=None,
    description='Проверка качества данных в nds.nds_sales',
    tags=['quality', 'nds']
) as dag:

    quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_data_quality_checks
    )
