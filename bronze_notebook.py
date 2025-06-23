# Bronze Layer - Ingest JSON from ADLS

from pyspark.sql.functions import col, to_date, lower

df_bronze = spark.read.parquet("abfss://retail@pocretailstorage1.dfs.core.windows.net/bronze/Vineet13161/E2E-Azure-Data-Engineer-Project--ADF-Spark-Synapse-ADLS-Azure-SQL/refs/heads/main/retail_transactions_bronze.parquet")
df_bronze.show()