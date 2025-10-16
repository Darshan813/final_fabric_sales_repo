# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import DataFrame
from pyspark.sql import functions as F



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_sales(df: DataFrame) -> DataFrame:

    # Rename columns to be consistent (e.g., "Total Amount" -> "total_amount")
    renamed_df = df
    for column in df.columns:
        new_col_name = column.replace(' ', '_').lower()
        renamed_df = renamed_df.withColumnRenamed(column, new_col_name)

    # Perform the transformations
    cleaned_df = (renamed_df
        .withColumn("gender",
            F.when(F.col("gender") == "Male", "M")
             .when(F.col("gender") == "Female", "F")
             .otherwise(F.col("gender"))
        )
       
        .withColumn("total_amount",
            F.round(F.col("quantity") * F.col("price_per_unit"), 2)
        )
        #.na.drop(subset=["total_amount"])
        .filter(F.col("total_amount") > 0)
    )

    cleaned_df = cleaned_df.drop('totalamount')
    return cleaned_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
