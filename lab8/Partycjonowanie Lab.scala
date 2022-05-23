// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(1)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(7)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(9)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(16)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(24)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(96)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(200)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.repartition(4000)
.groupBy("job").sum()


df.explain
print(df.count())

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(6)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(5)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(4)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(3)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(2)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/actorsParquet.parquet"

val df = spark.read
.parquet(parquetDir)
.coalesce(1)
.groupBy("job").sum()


df.explain
df.count()

df.rdd.getNumPartitions
