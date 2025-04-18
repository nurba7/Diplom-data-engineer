from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# Функция для создания всех схем и таблиц
def create_all_schemas_and_tables():
    engine = create_engine("postgresql+psycopg2://postgres:123@host.docker.internal:5432/postgres")

    ddl = """ 
    drop schema if exists sales cascade;
    drop schema if exists nds cascade;
    drop schema if exists dds cascade;

    create schema if not exists sales;
    create schema if not exists nds;
    create schema if not exists dds;

    create table if not exists sales.sales_data (
        invoice_id varchar(50) primary key,
        branch varchar(5),
        city varchar(50),
        customer_type varchar(50),
        gender varchar(10),
        product_line varchar(100),
        unit_price numeric,
        quantity int,
        tax_5_percent numeric,
        total numeric,
        date date,
        day int,
        month int,
        year int,
        weekday varchar(10),
        time time,
        payment_method varchar(50),
        cogs numeric,
        gross_margin_percentage numeric,
        gross_income numeric,
        rating numeric
    );

    create table if not exists nds.nds_customers (
        customer_id serial primary key,
        customer_type varchar(20),
        gender varchar(10),
        unique(customer_type, gender)
    );

    create table if not exists nds.nds_products (
        product_id serial primary key,
        product_line varchar(100),
        unit_price numeric(10, 2),
        unique(product_line, unit_price)
    );

    create table if not exists nds.nds_branches (
        branch_id serial primary key,
        branch_code varchar(10),
        city varchar(50),
        unique(branch_code, city)
    );

    create table if not exists nds.nds_dates (
        date_id serial primary key,
        full_date date,
        day int,
        month int,
        year int,
        weekday varchar(10)
    );

    create table if not exists nds.nds_sales (
        invoice_id varchar(20) primary key,
        customer_id int references nds.nds_customers(customer_id),
        product_id int references nds.nds_products(product_id),
        branch_id int references nds.nds_branches(branch_id),
        date_id int references nds.nds_dates(date_id),
        time time,
        quantity int,
        tax_5_percent numeric(10, 2),
        total numeric(10, 2),
        payment_method varchar(20),
        cogs numeric(10, 2),
        gross_margin_percentage numeric(5, 2),
        gross_income numeric(10, 2),
        rating numeric(3, 1)
    );

    create table if not exists dds.dim_customer (
        customer_id serial primary key,
        customer_type varchar(20),
        gender varchar(10)
    );

    create table if not exists dds.dim_product (
        product_id serial primary key,
        product_line varchar(50),
        unit_price numeric(10, 2)
    );

    create table if not exists dds.dim_branch (
        branch_id serial primary key,
        branch_code char(1),
        city varchar(50)
    );

    create table if not exists dds.dim_date (
        date_id serial primary key,
        date date,
        day int,
        month int,
        year int,
        weekday varchar(10)
    );

    create table if not exists dds.fact_sales (
        invoice_id varchar(20) primary key,
        customer_id int references dds.dim_customer(customer_id),
        product_id int references dds.dim_product(product_id),
        branch_id int references dds.dim_branch(branch_id),
        date_id int references dds.dim_date(date_id),
        time time,
        quantity int,
        total numeric(10, 2),
        tax_5_percent numeric(10, 2),
        gross_income numeric(10, 2),
        cogs numeric(10, 2),
        rating numeric(3, 1),
        payment_method varchar(20)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("Все схемы и таблицы успешно созданы.")

# Функция загрузки CSV-файла в таблицу sales_data
def load_data_into_sales():
    data = pd.read_csv('/opt/airflow/dags/files/sales.csv')

    data.columns = [
        'invoice_id', 'branch', 'city', 'customer_type', 'gender', 'product_line',
        'unit_price', 'quantity', 'tax_5_percent', 'total', 'date', 'time',
        'payment_method', 'cogs', 'gross_margin_percentage', 'gross_income', 'rating'
    ]
    # Обработка данных
    data = data.dropna()
    data['date'] = pd.to_datetime(data['date'], format='%m/%d/%Y')
    data['day'] = data['date'].dt.day
    data['month'] = data['date'].dt.month
    data['year'] = data['date'].dt.year
    data['weekday'] = data['date'].dt.day_name()
    data['time'] = pd.to_datetime(data['time'], format='%H:%M').dt.time

    #Преобразование названии колонны
    data = data[[  
        'invoice_id', 'branch', 'city', 'customer_type', 'gender', 'product_line',
        'unit_price', 'quantity', 'tax_5_percent', 'total', 'date', 'day', 'month',
        'year', 'weekday', 'time', 'payment_method', 'cogs',
        'gross_margin_percentage', 'gross_income', 'rating'
    ]]

    engine = create_engine("postgresql+psycopg2://postgres:123@host.docker.internal:5432/postgres")
    data.to_sql('sales_data', con=engine, schema='sales', if_exists='append', index=False)
    print("Данные успешно загружены в sales.sales_data.")

# DAG для создания таблиц и загрузки sales.csv
with DAG(
    dag_id="load_sales_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Создание схем и загрузка данных продаж"
) as dag:

    create_tables = PythonOperator(
        task_id='create_all_tables',
        python_callable=create_all_schemas_and_tables
    )

    load_sales = PythonOperator(
        task_id='load_sales_data',
        python_callable=load_data_into_sales
    )

    create_tables >> load_sales

