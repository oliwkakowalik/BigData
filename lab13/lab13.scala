// Databricks notebook source
// MAGIC %md
// MAGIC zadanie 2

// COMMAND ----------

// MAGIC %md
// MAGIC  W klastrze, w zakładce Metrics, pod live matrics, znajduje się Ganglia UI.

// COMMAND ----------

// MAGIC %md
// MAGIC zadanie 3

// COMMAND ----------

W ustawieniach klastra można zmniejszyć ilość pamięci przydzielonej dla np. spark.executor.memory na 1b lub spark.driver.memory na 1b

// COMMAND ----------

// MAGIC %md
// MAGIC zadanie 4

// COMMAND ----------

import org.apache.spark.sql.functions._

val path = "dbfs:/FileStore/tables/dataLab1/actors.csv"
var df = spark.read
            .format("csv")
            .option("header","true")
            .load(path)



val df_count = df.groupBy("category").count()

display(df_count)

// COMMAND ----------

df_count.write
  .format("parquet")
  .mode("overwrite")
  .bucketBy(5, "count")
  .saveAsTable("bucketedFiles")

// COMMAND ----------

df_count.write
  .format("parquet")
  .mode("overwrite")
  .partitionBy("count")
  .saveAsTable("partionFiles")

// COMMAND ----------

// MAGIC %md
// MAGIC zadanie 5

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE bucketedFiles COMPUTE STATISTICS NOSCAN;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE EXTENDED bucketedFiles;
