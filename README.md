# Airflow
Airflow ETL pipeline to pull Trial, Investigator and Organization data from Citeline. The schema and data for 3 entities are pulled and translated to be BigQuery compatible. Data is loaded as a json to maintain the nested format. Views are used to create a data model for reporting in Power BI.

![alt text](https://github.com/jakeduddy/Airflow/blob/main/Citeline%20DataModel.png?raw=true)
