from airflow.sdk import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import os

INPUT_DIR="/opt/airflow/data"
OUTPUT_DIR="/opt/airflow/output"
def extract_customer(ti):
    df=pd.read_csv(f"{INPUT_DIR}/customers.csv")
    print(df)
    ti.xcom_push(
        key="customers",
        value=df.to_dict("records")
    )

def validate_customer(ti):
    customers = ti.xcom_pull(
        task_ids="extract_customer",
        key="customers"
    )
    valid=[]
    for customer in customers:
        if customer.get("name") and customer.get("email"):
            valid.append(customer)
    print(f"Valid customers : {len(valid)}")
    ti.xcom_push(
        key="valid_customers",
        value=valid
    )


def load_customer(ti):
    valid_customer=ti.xcom_pull(
        task_ids="validate_customer",
        key="valid_customers"
    )
    # print(valid_customer)
    # print(type(valid_customer))
    os.makedirs(OUTPUT_DIR,exist_ok=True)
    with open(f"{OUTPUT_DIR}/validCustomers.txt","w") as f:
        for cust in valid_customer:
            f.write(f"{cust['customer_id']} {cust['name']}\n")

    print("Valid Customers loaded in database")

def send_welcome_email(ti):
    customers=ti.xcom_pull(
        task_ids="extract_customer",
        key="customers"
    )
    # print(customers)
    # print(type(customers))  
    os.makedirs(OUTPUT_DIR,exist_ok=True)
    with open (f"{OUTPUT_DIR}/email.txt","w") as f:
        for cust in customers:
            f.write(f"Welcome Customer {cust['name']}\n")

    print("Email sent")

with DAG(
    dag_id="customer_onboarding",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
) as dag :
    
    extract_task=PythonOperator(
        task_id="extract_customer",
        python_callable=extract_customer
    )
    validate_task=PythonOperator(
        task_id="validate_customer",
        python_callable=validate_customer
    )
    load_task=PythonOperator(
        task_id="load_customer",
        python_callable=load_customer
    )
    email_task=PythonOperator(
        task_id="email_sent",
        python_callable=send_welcome_email
    )

extract_task >> validate_task >> load_task
extract_task >> email_task