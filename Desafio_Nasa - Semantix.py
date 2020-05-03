import pyspark
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

df = spark.read.text("NASA_access_log_Aug95")
df1 = spark.read.text("NASA_access_log_Jul95")

df_concat = df.union(df1)
df_concat.show(5, truncate=False)

df2 = df_concat.select(split(col("value"),"- -")[0].alias("hostname"))

## 1. Número de hosts únicos.
distinctHosts = df2.select(countDistinct("hostname").alias("uniq_hosts"))
distinctHosts.show()

## 2. O total de erros 404.
df3 = df_concat.withColumn("cod_404", regexp_extract(df_concat.value, r"( 404 )", 0) )
df_cod_404 = df3.where(col("cod_404") == lit(" 404 "))
df_cod_404.count()

## 3. Os 5 URLs que mais causaram erro 404.
df4 = df_cod_404.select(split(col("value"),"- -")[0].alias("hostname"))

df4.groupBy("hostname").count().orderBy(col("count"),ascending=False).limit(5).show(truncate=False)

## 4. Quantidade de erros 404 por dia.
df5 = df_cod_404.select(split(col("value"),"]")[0].alias("aux_date"))
df6 = df5.select(split(col("aux_date"),"- -")[1].alias("date2"))
df6 = df6.withColumn('date', regexp_replace(df6.date2, '\[|:.*', '')).drop("date2")
df6.groupBy("date").count().orderBy(col("date")).show(10,False)

## 5. O total de bytes retornados.
df7 = df3.filter(col("cod_404") != '404')
df8 = df7.select(split(col("value"),'" ')[1].alias("aux_total_bytes"))
df9 = df8.select(split(col("aux_total_bytes")," ")[1].alias("aux_total_bytes2"))
df10 = df9.withColumn('total_bytes2', regexp_replace(df9.aux_total_bytes2, '-', '0')).drop("aux_total_bytes2")
df11 = df10.withColumn("total_bytes", df10["total_bytes2"].cast(IntegerType())).drop("total_bytes2")

## Primeira forma de realizar o group by
total_bytes = df11.groupBy().sum()
total_bytes.show()

## Segunda forma de realizar o group by
df11.agg(sum("total_bytes").alias("total_bytes")).show()
