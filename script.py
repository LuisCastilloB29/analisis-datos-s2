import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import FloatType, StringType, IntegerType, DateType
from pyspark.sql.functions import udf
import datetime
from datetime import datetime
import os

path_jar_driver = 'C:\Workspaces\mysql-connector-java-8.0.29.jar'

#Configuración de la sesión
conf=SparkConf() \
    .set('spark.driver.extraClassPath', path_jar_driver)

spark_context = SparkContext(conf=conf)
sql_context = SQLContext(spark_context)
spark = sql_context.sparkSession


db_user = 'Estudiante_15'
db_psswd = '4UBA5050J2'
db_connection_string = 'jdbc:mysql://157.253.236.116:8080/WWImportersTransactional'

df_bd = spark.read.format('jdbc')\
    .option('url', db_connection_string) \
    .option('dbtable', '''(SELECT * FROM Clientes) AS Compatible''') \
    .option('user', db_user) \
    .option('password', db_psswd) \
    .option('driver', 'com.mysql.cj.jdbc.Driver') \
    .load()

df_bd.show()