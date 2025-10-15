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

import pytest
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture to create a SparkSession for the entire test session.
    This makes the 'spark' object available to all test functions.
    """
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("TestRunner")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
