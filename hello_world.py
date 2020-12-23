#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print('Hello World')
    return 
def say_goodbye():
    print('Goodbye')
    return
def hello_again():
    print('Heeeellooo')
    return



dag = DAG('hello_world', description='Simple tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2020, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

goodbye_operator = PythonOperator(task_id='goodbye_task', python_callable=say_goodbye, dag=dag)

hello2_operator = PythonOperator(task_id='hello_task2', python_callable=hello_again, dag=dag)

dummy_operator >> hello_operator
dummy_operator >> goodbye_operator




# In[ ]:




