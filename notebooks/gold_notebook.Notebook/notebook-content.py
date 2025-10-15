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

from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Sales_Lakehouse.silver.silver_sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold = df.groupby('Year', 'Month', 'ProductCategory').agg(
    f.sum('TotalAmount').alias('Revenue'),
    f.sum('Quantity').alias('UnitsSold'),
    f.countDistinct('CustomerID').alias('UniqueCustomers')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold.write.format("delta").mode("overwrite").saveAsTable("gold.gold_sales_by_month_category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
