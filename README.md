# Expense Analysis Pipeline with Airflow

This repository contains code for an expense analysis pipeline implemented using Apache Airflow. The pipeline connects to Google Cloud Storage to retrieve expense data, performs data transformations and aggregations, and loads the results into BigQuery for further analysis.

## Pipeline Overview

The expense analysis pipeline consists of the following steps:

1. **Data Ingestion**: The pipeline retrieves expense data from Google Cloud Storage. The data is stored in CSV format and is divided into multiple files for different time periods and users.

2. **Data Transformation**: The pipeline performs various transformations on the data to prepare it for analysis. This includes cleaning the data, converting date fields to datetime format, handling missing values, and concatenating data from different sources.

3. **Dimension Table Creation**: The pipeline creates dimension tables to store additional information related to the expense data. This includes a datetime dimension table, a user dimension table, and a transaction type dimension table. These tables provide contextual information for analysis and reporting.

4. **Fact Table Creation**: The pipeline creates a fact table by joining the cleaned expense data with the dimension tables. The fact table contains the essential fields required for expense analysis, such as transaction date, user information, transaction type, and amounts.

5. **Loading Data to BigQuery**: The fact table and dimension tables are loaded into BigQuery to facilitate efficient querying and analysis. The data is written to dedicated tables within a specified BigQuery dataset.

## Prerequisites

Before running the pipeline, ensure that you have the following:

- Apache Airflow installed
- Google Cloud Storage account with access to the expense data
- Google Cloud project with BigQuery enabled

## Pipeline Configuration

1. Clone this repository to your local machine.

2. Update the pipeline configuration variables in the `lloyds_expense_pipeline.py` file:

   - `GCS_BUCKET`: Specify the Google Cloud Storage bucket where the expense data is stored.
   - `GCS_TRANSACTION_TYPE_FILE`: Specify the file path or name for the transaction type CSV file in the bucket.
   - `BQ_PROJECT_ID`: Specify the ID of your Google Cloud project.
   - `BQ_DATASET_NAME`: Specify the name of the BigQuery dataset where the tables will be created.
   - `BQ_FACT_TABLE_NAME`: Specify the name for the fact table in BigQuery.
   - `BQ_DATETIME_DIM_TABLE_NAME`: Specify the name for the datetime dimension table in BigQuery.
   - `BQ_USER_DIM_TABLE_NAME`: Specify the name for the user dimension table in BigQuery.
   - `BQ_TRANSTYPE_DIM_TABLE_NAME`: Specify the name for the transaction type dimension table in BigQuery.

3. Set up Apache Airflow and create a DAG by copying the `lloyds_expense_pipeline.py` file to your Airflow DAGs directory.

4. Configure the Airflow connection to Google Cloud Storage and BigQuery by following the Airflow documentation.

5. Start the Airflow webserver and scheduler.

6. Monitor the execution of the pipeline through the Airflow UI.

## Contributing

Contributions to this expense analysis pipeline are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

Feel free to customize the README.md file according to your specific needs.
