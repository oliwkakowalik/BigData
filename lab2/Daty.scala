// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` +
// MAGIC * `date_format()` +
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`+
// MAGIC * `to_date()` +
// MAGIC * `to_timestamp()` +
// MAGIC * `from_utc_timestamp()` +
// MAGIC * `to_utc_timestamp()`+

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------


dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------


val zmianaFormatu = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxxx", "yyyy-MM-dd HH:mm:ss") )

zmianaFormatu.printSchema()

// COMMAND ----------


val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)
// MAGIC https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
// MAGIC https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/

// COMMAND ----------

val withNewCols1 = dataFrame.withColumn("Year", year($"current_date").as("Year"))
                            .withColumn("Month", month($"current_date").as("Month"))
                            .withColumn("DayOfYear", dayofyear($"current_date").as("DayOfYear"))

val withNewCols2 =  dataFrame.withColumn("Year", date_format($"current_date", "yyyy"))
                            .withColumn("Month", date_format($"current_date", "MM"))
                            .withColumn("DayOfYear", date_format($"current_date", "D"))

display(withNewCols1)
display(withNewCols2)

// COMMAND ----------

//from_unixtime()
val fromUnixtime = dataFrame.
                  select($"unix", from_unixtime($"unix", "MM-dd-yyyy HH:mm:ss").as("timestamp"))
display(fromUnixtime)

// COMMAND ----------

//to_date string to date

val toDate = dataFrame.
                  select($"current_date", 
                  to_date($"current_date", "MM-dd-yyyy HH:mm:ss").as("toDate"))
display(toDate)

// COMMAND ----------

//to_timestamp() convert String to Timestamp Type

val toTimestamp = dataFrame.
                  select($"current_date", 
                  to_timestamp($"current_date", "yyyy-MM-dd HH:mm:ss").as("toTimestamp"))
display(toTimestamp)

// COMMAND ----------

//to_utc_timestamp() This function takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given timezone, and renders that timestamp as a timestamp in UTC.

val toUTC = dataFrame.
                  select($"current_date", 
                  to_utc_timestamp($"current_date", "PST").as("toUTC"))
display(toUTC)


// COMMAND ----------

//from_utc_timestamp() This function takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and renders that timestamp as a timestamp in the given time zone.

val fromUTC = toUTC.
                  select($"toUTC", 
                  to_timestamp($"toUTC", "PST").as("fromUTC"))
display(fromUTC)
