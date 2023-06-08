from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python import PythonOperator
import pandas as pd

# Read the file from Google Cloud Storage bucket
resh22 = pd.read_csv('gs://expense_dashboard/resh2022.csv')
resh23 = pd.read_csv('gs://expense_dashboard/resh2023.csv')
arun22 = pd.read_csv('gs://expense_dashboard/arun2022.csv')
arun23 = pd.read_csv('gs://expense_dashboard/arun2023.csv')
transactiontype = pd.read_csv('gs://expense_dashboard/transactionType.csv')

# Create user dimension table
user_data = {'user_id': [1, 2], 'user_name': ['Reshma', 'Arun']}
user_dim = pd.DataFrame(data=user_data)

def create_fact_table():
    # Concatenate Reshma's and Arun's data
    reshdf = pd.concat([resh22, resh23], axis=0)
    arundf = pd.concat([arun22, arun23], axis=0)

    # Convert "Transaction Date" columns to datetime
    reshdf["Transaction Date"] = pd.to_datetime(reshdf["Transaction Date"])
    arundf["Transaction Date"] = pd.to_datetime(arundf["Transaction Date"])

    # Replace NaN values with 0
    reshdf = reshdf.fillna(0)
    arundf = arundf.fillna(0)

    # Create datetime dimension table
    datetime_dim = pd.DataFrame()
    datetime_dim["transaction_date"] = pd.concat([reshdf["Transaction Date"], arundf["Transaction Date"]]).drop_duplicates().reset_index(drop=True)
    datetime_dim['day'] = datetime_dim['transaction_date'].dt.day
    datetime_dim['month'] = datetime_dim['transaction_date'].dt.month
    datetime_dim['year'] = datetime_dim['transaction_date'].dt.year
    day_name = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday"
    }
    datetime_dim['weekday'] = datetime_dim['transaction_date'].dt.weekday.map(day_name)
    datetime_dim['date_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['date_id', 'day', 'month', 'year', 'weekday', 'transaction_date']]

    # Create transaction type dimension table
    transtype_dim = transactiontype
    transtype_dim['tran_id'] = transtype_dim.index
    transtype_dim = transtype_dim[['tran_id', 'Code', 'Description']]

    # Create fact table
    main_df = pd.concat([reshdf, arundf]).drop_duplicates().reset_index(drop=True)
    fact_table = pd.merge(main_df, datetime_dim, left_on='Transaction Date', right_on='transaction_date')
    fact_table = pd.merge(fact_table, user_dim, on='user_id')
    fact_table = pd.merge(fact_table, transtype_dim, left_on='Transaction Type', right_on='Code')

    # Select the required columns for the fact table
    fact_table = fact_table[['date_id', 'user_id', 'tran_id', 'Credit Amount', 'Debit Amount', 'Balance']]
    fact_table['ex_id'] = fact_table.index
    # Rename the columns in the fact table
    fact_table.columns = ['date_id', 'user_id', 'tran_id', 'credit_amt', 'debit_amt', 'balance', 'ex_id']

    # Reorder columns in the fact table
    fact_table = fact_table[['ex_id', 'date_id', 'user_id', 'tran_id', 'credit_amt', 'debit_amt', 'balance']]

    # Write the fact table to a CSV file
    fact_table.to_csv('/path/to/fact_table.csv', index=False)

def write_dimension_tables():
    # Write datetime_dim to BigQuery
    datetime_dim.to_gbq(
        'reshma-project-2604.lloyds_expense_analysis.datetime_dim',
        project_id='Reshma-project-2604',
        if_exists='replace'
    )

    # Write user_dim to BigQuery
    user_dim.to_gbq(
        'reshma-project-2604.lloyds_expense_analysis.user_dim',
        project_id='Reshma-project-2604',
        if_exists='replace'
    )

    # Write transtype_dim to BigQuery
    transtype_dim.to_gbq(
        'reshma-project-2604.lloyds_expense_analysis.transtype_dim',
        project_id='reshma-project-2604',
        if_exists='replace'
    )



# Define the DAG
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('my_pipeline', default_args=default_args, schedule_interval='0 0 * * *')

# Create a PythonOperator to execute the create_fact_table function
create_fact_table_task = PythonOperator(
    task_id='create_fact_table',
    python_callable=create_fact_table,
    dag=dag
)

# Create a PythonOperator to execute the write_dimension_tables function
write_dimension_tables_task = PythonOperator(
    task_id='write_dimension_tables',
    python_callable=write_dimension_tables,
    dag=dag
)

# Create a GoogleCloudStorageToBigQueryOperator to load the fact table into BigQuery
load_to_bigquery_task = GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket='expense_dashboard',
    source_objects=['/path/to/fact_table.csv'],
    destination_project_dataset_table='reshma-project-2604.lloyds_expense_analysis.table',
    write_disposition='WRITE_TRUNCATE',  # Choose the write disposition (e.g., WRITE_TRUNCATE, WRITE_APPEND, etc.)
    dag=dag
)

# Set the task dependencies
create_fact_table_task >> write_dimension_tables_task >> load_to_bigquery_task