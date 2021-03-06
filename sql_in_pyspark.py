# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, DateType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType

spark = SparkSession.builder.appName("sql_in_pyspark").getOrCreate()

print('spark session was created!')


schema=StructType([
		StructField("Konum",StringType(),True),
		StructField("Lokasyon_Sayisi",IntegerType(),True),
		StructField("Yerli_Abone_Sayisi",IntegerType(),True),
		StructField("Yabanci_Abone_Sayisi",IntegerType(),True)])

sourceDataDf= spark.read.format("csv")\
              .option("header","true")\
              .schema(schema)\
	          .option("sep",",")\
	          .option("mode","failfast")\
	          .load("ibbwifi-ilce-veya-mobil-lokasyona-gore-yeni-abonelik-istatistikleri.csv")

sourceDataDf.show(1)


sourceDataDf.createOrReplaceTempView("source_data")
#sourceDataDf.printSchema()

print('source data temp was created !')
spark.sql("select *  from source_data").show(5)
print('selected data from  source_data temp !')


df_calc_recs="""
          SELECT \
          s1.*
          from source_data s1
          where s1.Yerli_Abone_Sayisi > s1.Yabanci_Abone_Sayisi
          order by s1.Yerli_Abone_Sayisi desc
          """

df_calc_recs = spark.sql(df_calc_recs)

df_calc_recs.write.format('csv')\
.option('header','true')\
.mode('overwrite')\
.save('df_calc_recs.csv')


spark.stop()
