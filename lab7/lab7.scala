// Databricks notebook source
// MAGIC %md zadanie 1
// MAGIC <br>
// MAGIC Hive ma ograniczone możliwości odnośnie indeksowania. Mimo, iż nie ma kluczy (primary key w relacyjnych bazach danych) to można stworzyć indeks na kolumnach, które są przechowywane w innej tabeli. Jednak utrzymanie takiego indeksu wymaga dodatkowego miejsca na dysku, a jego budowanie  jest związane z kosztami przetwarzania, dlatego należy porównać koszty z korzyściami płynącymi z indeksowania.
// MAGIC Jest alternatywą dla partycjonowania, gdy partycje logiczne byłyby w rzeczywistości zbyt liczne i małe, aby były użyteczne. Jest ono użyteczne w oczyszczeniu niektórych bloków z tabeli jako danych wejściowych dla zadania MapReduce. 

// COMMAND ----------

// MAGIC %md
// MAGIC zadanie 3

// COMMAND ----------

spark.catalog.listDatabases()

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS db")

// COMMAND ----------

val path = "dbfs:/FileStore/tables/dataLab1/names.csv"
val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(path)

df.write.mode("overwrite").saveAsTable("db.names")

// COMMAND ----------

val path = "dbfs:/FileStore/tables/dataLab1/actors.csv"
val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(path)

df.write.mode("overwrite").saveAsTable("db.actors")

// COMMAND ----------

spark.catalog.listDatabases().show()

// COMMAND ----------

spark.catalog.listTables("db").show

// COMMAND ----------

def function(database: String){
  
  val tables = spark.catalog.listTables(s"$database")
  val names=tables.select("name").as[String].collect.toList
  var i = List()
  for( i <- names){
    spark.sql(s"DELETE FROM $database.$i")
  }
  
}

// COMMAND ----------

function("db")

// COMMAND ----------

spark.catalog.listTables("db").show
