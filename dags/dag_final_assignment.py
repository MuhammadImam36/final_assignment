# import library/dependencies
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow.utils.dates import days_ago
from utils.scraper import BPNScraper

@dag(
    dag_id="dag_final_assignment",
    description="DAG etl untuk final_assignment",
    tags=["assignment"],
    default_args={
        "owner": "Imam",
        "retry_delay": timedelta(minutes=5)
    },
    owner_links={
        "Imam": "https://www.linkedin.com/in/mimamwahid/"
    },
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=days_ago(1)
)
def main():
    url = "https://api-panelhargav2.badanpangan.go.id/api/front/harga-peta-provinsi"
    params = {
        "level_harga_id": 3,
        "komoditas_id": 109,
        "period_date": "13/05/2025"
    }
    scraper = BPNScraper(url, params=params)
    status = scraper.metadata_status
    provinsi = scraper.metadata_provinsi
    komoditas = scraper.metadata_komoditas
    harian = scraper.data_harga_harian

    start = EmptyOperator(task_id="start")
    
    extract_metadata_status = PythonOperator(
        task_id='extract_metadata_status',
        python_callable=status
    )
    
    extract_metadata_provinsi = PythonOperator(
        task_id='extract_metadata_provinsi',
        python_callable=provinsi
    )
    
    extract_metadata_komoditas = PythonOperator(
        task_id='extract_metadata_komoditas',
        python_callable=komoditas
    )
    
    extract_data_harga_harian = PythonOperator(
        task_id='extract_data_harga_harian',
        python_callable=harian
    )
    
    transform = SparkSubmitOperator(
        application="/spark-scripts/spark_final_assignment.py",
        conn_id="spark_main",
        task_id="transform_data",
        trigger_rule = TriggerRule.ALL_SUCCESS,
        packages="org.postgresql:postgresql:42.2.18",
        env_vars={
            "POSTGRES_DWH": "pangan_indonesia_2025",
            "POSTGRES_USER": "user",
            "POSTGRES_PASSWORD": "password",
            "POSTGRES_CONTAINER_NAME": "dataeng-postgres"
        },
    )
    
    load = EmptyOperator(task_id='load')
    end = EmptyOperator(task_id="end")   
    
    start >> [extract_metadata_status, extract_metadata_provinsi, 
            extract_metadata_komoditas, extract_data_harga_harian] >> transform >> load >> end

main()