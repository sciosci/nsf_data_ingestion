#!/bin/bash

# Make sure you define the environmental variables SPARK_HOME,
# PYSPARK_DRIVER_PYTHON, and PYSPARK_PYTHON.
# For example:
# SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
# PYSPARK_DRIVER_PYTHON=/home/deacuna/anaconda3/bin/python
# PYSPARK_PYTHON=/home/deacuna/anaconda3/bin/python
spark2-submit "$@"
