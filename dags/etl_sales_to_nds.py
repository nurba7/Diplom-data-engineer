from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Создаём даг
dag = DAG(
    dag_id='etl_sales_to_nds',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL из sales_data в NDS'
)

# Подключение к БД где лежит исходная схема и схема nds
def get_conn():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="123",
        host="host.docker.internal",
        port="5432",
        options='-c search_path=dds,nds,sales'
    )

# Проверка соединения
def test_connection():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("select version();")
    print(cur.fetchone())
    cur.close()
    conn.close()

test = PythonOperator(task_id='test_connection', python_callable=test_connection, dag=dag)

#Загрузка данных в nds_customers
def load_nds_customers():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from nds.nds_customers c")
    cur.execute("""
        insert into nds.nds_customers (customer_type, gender)
        select distinct customer_type, gender from sales.sales_data
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to nds_customers")

#Загрузка данных в nds_products
def load_nds_products():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from nds.nds_products p")
    cur.execute("""
        insert into nds.nds_products (product_line, unit_price)
        select distinct product_line, unit_price from sales.sales_data
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to nds_products ")

#Загрузка данных в nds_branches
def load_nds_branches():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from nds.nds_branches b")
    cur.execute("""
        insert into nds.nds_branches (branch_code, city)
        select distinct branch, city from sales.sales_data
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to nds_branches")

#Загрузка данных в nds_dates
def load_nds_dates():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from nds.nds_dates d")
    cur.execute("""
        insert into nds.nds_dates (full_date, day, month, year, weekday)
        select distinct date, day, month, year, weekday from sales.sales_data
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to nds_dates")

#Загрузка данных в nds_sales
def load_nds_sales():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from nds.nds_sales")
    cur.execute("""
        insert into nds.nds_sales (
            invoice_id, customer_id, product_id, branch_id, date_id, time,
            quantity, tax_5_percent, total, payment_method, cogs,
            gross_margin_percentage, gross_income, rating
        )
        select distinct
            s.invoice_id,
            c.customer_id,
            p.product_id,
            b.branch_id,
            d.date_id,
            s.time,
            s.quantity,
            s.tax_5_percent,
            s.total,    
            s.payment_method,
            s.cogs,
            s.gross_income,     
            s.gross_margin_percentage,
            s.rating
        from sales.sales_data s
        join nds_customers c on s.customer_type = c.customer_type and s.gender = c.gender
        join nds_products p on s.product_line = p.product_line and s.unit_price = p.unit_price
        join nds_branches b on s.branch = b.branch_code and s.city = b.city
        join nds_dates d on s.date = d.full_date        
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to nds_sales")

#Создаём таски для выполнения загрузки по зависимости
t1 = PythonOperator(task_id='load_customers', python_callable=load_nds_customers, dag=dag)
t2 = PythonOperator(task_id='load_products', python_callable=load_nds_products, dag=dag)
t3 = PythonOperator(task_id='load_branches', python_callable=load_nds_branches, dag=dag)
t4 = PythonOperator(task_id='load_dates', python_callable=load_nds_dates, dag=dag)
t5 = PythonOperator(task_id='load_sales', python_callable=load_nds_sales, dag=dag)

# Зависимости: сначала всё загрузим в размерности, потом в nds_sales
test >> [t1, t2, t3, t4] >> t5