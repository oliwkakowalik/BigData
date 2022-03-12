// Databricks notebook source
//exercise 1
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField, ArrayType}
import org.apache.spark.sql.{Row, SparkSession}

val simpleSchema = StructType(Array(
  StructField("imdb_title_id", StringType, false),
  StructField("ordering", IntegerType, false),
  StructField("imdb_name_id", StringType, false), 
  StructField("category", StringType, false),
  StructField("job", StringType, true),
  StructField("characters", StringType, true)
))

val path = "/FileStore/tables/dataLab1/actors.csv"
val dataFrame = spark.read.option("delimiter", ",").option("header", "true").schema(simpleSchema).csv(path)
//df.printSchema

// COMMAND ----------

//exercise 4
val r1 = "{00000}"
val r2 = "{'imdb_title_id': 'tt00003', 'ordering': 1, 'imdb_name_id': 'nm0063486', 'category': 'actress', 'job': 'null', 'characters': ['Miss Geraldine Holbrook (Miss Jerry)']}"
val r3 = "{'imdb_title_id': 'tt0110009', 'ordering': 2, 'imdb_name_id': 'nm003086', 'category': 1, 'job': 'null', 'characters': 'Miss Geraldine Holbrook (Miss Jerry)'}"

Seq(r1, r2, r3).toDF().write.mode("overwrite").text("/FileStore/tables/dataLab1/zad4.json")

val path = "/FileStore/tables/dataLab1/zad4.json"

val df = spark.read.format("json")
  .schema(simpleSchema)
  .option("mode", "permissive")
  .load(path)

//display(df)
//all fields are set to null and corrupted record is placed in a string column called _corrupt_record

val df2 = spark.read.format("json")
  .schema(simpleSchema)
  .option("mode", "dropMalformed")
  .load(path)

//display(df2)
//corrupted row was dropped

val df3 = spark.read.format("json")
  .schema(simpleSchema)
  .option("mode", "failFast")
  .load(path)

//display(df3)
//failFast mode fails, because 1st record is corrupt

val df4 = spark.read.format("json")
  .schema(simpleSchema)
  .option("badRecordsPath", "/FileStore/tables/badrecords")
  .load(path)

//df4.show()
//df4 contains 2 proper records, corrupted one is placed in included badRecordsPath

// COMMAND ----------

//exercise 5
miniDf.write.format("parquet").mode("overwrite").save("/FileStore/tables/actorsParquet.parquet")
val actorsParquet = spark.read.format("parquet").load("/FileStore/tables/actorsParquet.parquet")
display(actorsParquet)
//when I come to file I see only incomprehensible signs but when I open it, it contains everything it should
