# Airflow
Airflow ETL pipeline to pull Trial, Investigator and Organization data from Citeline. Data and schema is pull for these 3 entities, translated to be BigQuery compatible. Data is loaded as a json to maintain the nested format. Views are used to create a data model for reporting in Power BI.
