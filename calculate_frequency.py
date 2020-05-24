# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, DateType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType

spark = SparkSession.builder.appName("frequency_calculation").getOrCreate()

#spark.getConf().getAll()

print('spark session was created!')

schema=StructType([
		StructField("User_ID",StringType(),True),
		StructField("Session_ID",StringType(),True),
        StructField("Year",IntegerType(),True),
		StructField("Month",IntegerType(),True),
		StructField("Day",IntegerType(),True),
		StructField("Hour",IntegerType(),True),
		StructField("Product_ID",StringType(),True),
        ])

sourceDataDf= spark.read.format("csv")\
              .option("header","false")\
              .schema(schema)\
	          .option("sep","\t")\
	          .option("mode","failfast")\
	          .load("sample.csv")

sourceDataDf.show(1)


sourceDataDf.createOrReplaceTempView("source_data")
#sourceDataDf.printSchema()

print('source data temp was created !')
spark.sql("select *  from source_data").show(5)
print('selected data from  source_data temp !')


df_frequency_recs="""
          SELECT \
          s1.Product_ID as P_ID1, s2.Product_ID as P_ID2, count(*) as frequency \
          from source_data s1 join source_data s2
          on s1.User_ID = s2.User_ID
          and s1.Session_ID=s2.Session_ID
          where s1.Product_ID != s2.Product_ID
          and s1.Product_ID not like '%\\N%'
          and s2.Product_ID not like '%\\N%'
          group by s1.Product_ID, s2.Product_ID
          order by frequency desc
          """

df_frequency_recs = spark.sql(df_frequency_recs)

df_frequency_recs.write.format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .option("confirm.truncate", "true")
  .option("spark.cassandra.connection.host", "10.142.0.3")
  .option("spark.cassandra.connection.port", "9042")
  .option("keyspace", "sparkdb")
  .option("table", "frequency_result")
  .save()

spark.stop()



