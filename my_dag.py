import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'zoo_etl_pipeline',
    default_args=default_args,
    description='ETL pipiline',
    schedule_interval='@daily',
)


dag_directory = os.path.dirname(os.path.abspath(__file__)) 

def extract_zoo_animals(**kwargs):
    file_path = os.path.join(dag_directory, 'zoo_animals.csv')
    logging.info(f"Extracting {file_path}")
    zoo_animals_df = pd.read_csv(file_path)
    logging.info(f"Extracted {len(zoo_animals_df)} records from zoo_animals.csv")
    return zoo_animals_df

def extract_zoo_health_records(**kwargs):
    file_path = os.path.join(dag_directory, 'zoo_health_records.csv')
    logging.info(f"Extracting {file_path}")
    zoo_health_records_df = pd.read_csv(file_path)
    logging.info(f"Extracted {len(zoo_health_records_df)} records from zoo_health_records.csv")
    return zoo_health_records_df

def merge_data(ti, **kwargs):
    zoo_animals_df = ti.xcom_pull(task_ids='extract.extract_zoo_animals')
    zoo_health_records_df = ti.xcom_pull(task_ids='extract.extract_zoo_health_records')
    
    logging.info("Merging data")
    merged_df = pd.merge(zoo_animals_df, zoo_health_records_df, on='animal_id')
    logging.info(f"Merged data has {len(merged_df)} records")
    return merged_df

def filter_age(ti, **kwargs):
    merged_df = ti.xcom_pull(task_ids='transform.merge_data')
    
    logging.info("Filtering data by age")
    filtered_df = merged_df[merged_df['age'] < 2]  
    logging.info(f"Filtered data has {len(filtered_df)} records")
    return filtered_df

def transform_animal_name(ti, **kwargs):
    filtered_df = ti.xcom_pull(task_ids='transform.filter_age')
    
    logging.info("Transforming animal names to title case")
    filtered_df['animal_name'] = filtered_df['animal_name'].str.title()
    logging.info("Animal names transformed")
    return filtered_df

def filter_health_status(ti, **kwargs):
    filtered_df = ti.xcom_pull(task_ids='transform.transform_animal_name')
    
    logging.info("Filtering data by health status")
    transformed_df = filtered_df[filtered_df['health_status'].isin(['Healthy', 'Needs Attention'])]
    logging.info(f"Filtered health status data has {len(transformed_df)} records")
    return transformed_df

def aggregate_data(ti, **kwargs):
    transformed_df = ti.xcom_pull(task_ids='transform.filter_health_status')

    logging.info("Aggregating data")
    species_count = transformed_df['species'].value_counts().to_dict()
    health_status_count = transformed_df['health_status'].value_counts().to_dict()
    
    logging.info(f"Species count: {species_count}")
    logging.info(f"Health status count: {health_status_count}")
    
    aggregated_data = {
        'species_count': species_count,
        'health_status_count': health_status_count
    }
    
    logging.info("Aggregated data successfully")
    return transformed_df, aggregated_data

def validate_data(ti, **kwargs):
    transformed_df, aggregated_data = ti.xcom_pull(task_ids='aggregate_data')

    logging.info("Validating aggregated data")
    species_count = aggregated_data['species_count']
    health_status_count = aggregated_data['health_status_count']
    
    assert sum(species_count.values()) == len(transformed_df), "Species count validation failed"
    
    
    required_health_statuses = ['Healthy', 'Needs Attention']
    missing_health_statuses = [status for status in required_health_statuses if status not in health_status_count]
    
    if missing_health_statuses:
        logging.warning(f"Health status validation warning, missing statuses: {missing_health_statuses}")

    
    if 'Healthy' not in health_status_count and 'Needs Attention' not in health_status_count:
        raise AssertionError("Health status validation failed: Neither 'Healthy' nor 'Needs Attention' found.")
    
    logging.info("Validated data successfully")
    return transformed_df, aggregated_data

def load_data(ti, **kwargs):
    transformed_df, aggregated_data = ti.xcom_pull(task_ids='validate_data')

    output_path = os.path.join(dag_directory, 'final_zoo_data.csv')
    logging.info(f"Loading data to {output_path}")
    transformed_df.to_csv(output_path, index=False)
    logging.info("Data loaded successfully")

with dag:
    with TaskGroup("extract") as extract_group:
        extract_zoo_animals_task = PythonOperator(
            task_id='extract_zoo_animals',
            python_callable=extract_zoo_animals,
            provide_context=True,
            dag=dag,
        )

        extract_zoo_health_records_task = PythonOperator(
            task_id='extract_zoo_health_records',
            python_callable=extract_zoo_health_records,
            provide_context=True,
            dag=dag,
        )

    with TaskGroup("transform") as transform_group:
        merge_data_task = PythonOperator(
            task_id='merge_data',
            python_callable=merge_data,
            provide_context=True,
            dag=dag,
        )

        filter_age_task = PythonOperator(
            task_id='filter_age',
            python_callable=filter_age,
            provide_context=True,
            dag=dag,
        )

        transform_animal_name_task = PythonOperator(
            task_id='transform_animal_name',
            python_callable=transform_animal_name,
            provide_context=True,
            dag=dag,
        )

        filter_health_status_task = PythonOperator(
            task_id='filter_health_status',
            python_callable=filter_health_status,
            provide_context=True,
            dag=dag,
        )

    aggregate = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        provide_context=True,
        dag=dag,
    )

    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
        dag=dag,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        dag=dag,
    )

    extract_group >> transform_group >> aggregate >> validate >> load

