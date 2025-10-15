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

import yaml 
import re
from io import StringIO
from datetime import datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

yaml_path = 'abfss://sales_dev@onelake.dfs.fabric.microsoft.com/Sales_Lakehouse.Lakehouse/Files/config.yaml'

# read file as lines then join to single string
lines = spark.sparkContext.textFile(
    yaml_path
).collect()
text = "\n".join(lines)

# parse YAML
config = yaml.safe_load(text)
print(config)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

today = datetime.today()
yyyy = today.strftime("%Y")
MM = today.strftime("%m")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for key, values in config.items():
    for i in values:
        name = i['name']
        path = i['path'].format(yyyy = yyyy, MM = MM)

        df = (
        spark.read
        .format(i['file_format'])
        .option("header", i["header"])
        .option("inferSchema", i["inferSchema"])
        .option("delimiter", i["delimiter"])
        .load(path)
        )

        cleaned_columns = [re.sub(r'[^A-Za-z0-9_]+', '', col.replace(' ', '_')).lower() for col in df.columns]
        df_cleaned = df.toDF(*cleaned_columns)
        df_cleaned.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{i['name']}")

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
