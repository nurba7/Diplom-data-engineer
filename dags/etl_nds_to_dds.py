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
    dag_id='etl_nds_to_dds',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL из NDS в DDS'
)

# Подключение к БД где лежит исходная схема и схема nds
def get_conn():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="123",
        host="host.docker.internal",
        port="5432",
        options='-c search_path=dds,nds,sales_data'
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

#Загрузка данных в dim_customer
def load_dds_customers():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from dds.dim_customer")
    cur.execute("""
        insert into dim_customer (customer_type, gender)
        select distinct customer_type, gender from nds.nds_customers
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to dim_customer")

#Загрузка данных в dim_product
def load_dds_products():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from dds.dim_product")
    cur.execute("""
        insert into dim_product (product_line, unit_price)
        select distinct product_line, unit_price from nds.nds_products
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to dim_product")

#Загрузка данных в dim_branch
def load_dds_branches():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from dds.dim_branch")
    cur.execute("""
        insert into dim_branch (branch_code, city)
        select branch_code, city from nds.nds_branches
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to dim_branch")

#Загрузка данных в dim_dates
def load_dds_dates():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from dds.dim_date")
    cur.execute("""
        insert into dim_date (date, day, month, year, weekday)
        select distinct full_date, day, month, year, weekday from nds.nds_dates
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to dim_date")

#Загрузка данных в dim_sales
def load_dds_sales():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("delete from dds.fact_sales s")
    cur.execute("""
        insert into fact_sales (
            invoice_id, customer_id, product_id, branch_id, date_id, time,
            quantity, total, tax_5_percent, gross_income, cogs, rating, payment_method
        )
        select distinct
            ns.invoice_id,
            dc.customer_id,
            dp.product_id,
            db.branch_id,
            dd.date_id,
            ns.time,
            ns.quantity,
            ns.total,
            ns.tax_5_percent,
            ns.gross_income,
            ns.cogs,
            ns.rating,
            ns.payment_method
        from nds.nds_sales ns
        join nds.nds_customers nc on ns.customer_id = nc.customer_id
        join dim_customer dc on nc.customer_type = dc.customer_type and nc.gender = dc.gender
        join nds.nds_products np on ns.product_id = np.product_id
        join dim_product dp on np.product_line = dp.product_line and np.unit_price = dp.unit_price
        join nds.nds_branches nb on ns.branch_id = nb.branch_id
        join dim_branch db on nb.branch_code = db.branch_code and nb.city = db.city
        join nds.nds_dates nd on ns.date_id = nd.date_id
        join dim_date dd on nd.full_date = dd.date;          
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Succesfully loaded to fact_sales")

#Создаём таски для выполнения загрузки по зависимости
t1 = PythonOperator(task_id='load_customers', python_callable=load_dds_customers, dag=dag)
t2 = PythonOperator(task_id='load_products', python_callable=load_dds_products, dag=dag)
t3 = PythonOperator(task_id='load_branches', python_callable=load_dds_branches, dag=dag)
t4 = PythonOperator(task_id='load_dates', python_callable=load_dds_dates, dag=dag)
t5 = PythonOperator(task_id='load_sales', python_callable=load_dds_sales, dag=dag)

# Зависимости: сначала всё загрузим в размерности, потом в факт
[t1, t2, t3, t4] >> t5