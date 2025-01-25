"""
Test DAG to verify loading works.
"""
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id='test_scraper',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def test_scraper_dag():
    
    @task
    def print_hello():
        print("Hello from test DAG!")
        
    print_hello()

# Instantiate the DAG
test_scraper_dag() 