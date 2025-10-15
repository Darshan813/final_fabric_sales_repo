# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e9a81e65-8046-4f7f-b3c8-0ffdf3bcd943",
# META       "default_lakehouse_name": "Sales_Lakehouse",
# META       "default_lakehouse_workspace_id": "ff9fc356-755f-447d-8c6b-ccbd95cc0f83",
# META       "known_lakehouses": [
# META         {
# META           "id": "e9a81e65-8046-4f7f-b3c8-0ffdf3bcd943"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
print('Bheruji')
print('Bheruji')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install kagglehub

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import kagglehub
from kagglehub import KaggleDatasetAdapter

df = kagglehub.load_dataset(
  KaggleDatasetAdapter.PANDAS,
  "mohammadtalib786/retail-sales-dataset",
  "retail_sales_dataset.csv",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns = ['TransactionID', 'Date', 'CustomerID', 'Gender', 'Age','ProductCategory', 'Quantity', 'price_per_unit', 'TotalAmount']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_spark = spark.createDataFrame(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_spark.write.format("delta").mode("overwrite").saveAsTable("bronze.sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
