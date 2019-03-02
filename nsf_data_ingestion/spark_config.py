import findspark
findspark.init('/usr/local/spark')
#findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
import pyspark
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import rank, max, sum, desc
import zlib
import importlib
import os
from os import path
from shutil import copyfile
from shutil import rmtree
from subprocess import call
import logging



spark_home='/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/'
appname='nsf_data_engine'
exec_instance='1'
exec_mem='8g'
exec_cores='2'
exec_max_cores='2'

#HDFS Parameters
minPartitions = 10000
