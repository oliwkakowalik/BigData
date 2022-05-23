// Databricks notebook source
// MAGIC %md
// MAGIC #Partycjonowanie
// MAGIC 
// MAGIC * Wikipedia odwiedziny strony
// MAGIC * Rozmiar ~255 MB
// MAGIC 
// MAGIC 
// MAGIC * Różnice pomiędzy partycjami a slots/cores
// MAGIC * Porównanie `repartition(n)` and `coalesce(n)`
// MAGIC * Shuffle partitions

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//val schema = StructType(
  //List(
    //StructField("timestamp", StringType, false),
    //StructField("site", StringType, false),
    //StructField("requests", IntegerType, false)
  //)
//)

val fileName = "dbfs:/FileStore/tables/dataLab1/names.csv"

val initialDF = spark.read
  .option("header", "true")
  .option("sep", ",")
  .option("inferSchema","true")
  .csv(fileName)
initialDF.printSchema()

// COMMAND ----------

//display(initialDF.orderBy("imdb_name_id"))
initialDF.count() // / 75419

// COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Partycje kontra Sloty
// MAGIC 
// MAGIC ** *The Spark API uses the term **core** meaning a thread available for parallel execution.*<br/>*Here we refer to it as **slot** to avoid confusion with the number of cores in the underlying CPU(s)*<br/>*to which there isn't necessarily an equal number.*

// COMMAND ----------

// MAGIC %md
// MAGIC ### Slots/Cores
// MAGIC 
// MAGIC Sprawdzam ile jest slotów `SparkContext.defaultParallelism`
// MAGIC 
// MAGIC Dokumentacja <a href="https://spark.apache.org/docs/latest/configuration.html#execution-behavior" target="_blank">Spark Configuration, Execution Behavior</a>
// MAGIC 
// MAGIC > Może zależeć od manager clustra:
// MAGIC > * Local mode: number of cores on the local machine
// MAGIC > * Mesos fine grained mode: 8
// MAGIC > * **Others: total number of cores on all executor nodes or 2, whichever is larger**

// COMMAND ----------

sc.defaultParallelism //ilosc slotów

// COMMAND ----------

// MAGIC %md
// MAGIC ### Partitions
// MAGIC 
// MAGIC * Ile jest partycji
// MAGIC 
// MAGIC Dataset zawiera 7M wierszy to 7/8 = 900 000 rekordów na parytcję

// COMMAND ----------

// MAGIC %md
// MAGIC 75419 wierszy, to 75419/8=9427 rekordów na partycję

// COMMAND ----------

initialDF.count() 

// COMMAND ----------

// MAGIC %md
// MAGIC Jak sprawdzić ilość partycji 
// MAGIC * wykonaj konwersję do `RDD`
// MAGIC * zapytaj o `RDD` ilość partycji 

// COMMAND ----------

val partycje = initialDF.rdd.getNumPartitions //ilość partycji

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC * To nie przypadek źe mam **8 slots** i **8 partitions**
// MAGIC * Spark sprawdza ile jest **slots**, i na rozmiar danych i domyślnie ustawia ilość partycji.
// MAGIC * Nawet jeśli zwiększe ilość danych Spark wczyta **8 partycji**.
// MAGIC </br>
// MAGIC 
// MAGIC * Wczytuję kopię danych ale już podzielonych na partycję

// COMMAND ----------

val alternateDF = spark.read
  .format("parquet").load("/FileStore/tables/actorsParquet.parquet")

printf("Partitions: %d%n%n", alternateDF.rdd.getNumPartitions) //jedna partycja

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/actors_p.parquet

// COMMAND ----------

// MAGIC %md
// MAGIC Teraz mam 5 partycji i wykonam akcję count **8 slotów i 5 partycji**

// COMMAND ----------

// MAGIC %md 
// MAGIC U mnie 2 partycje, wykonam akcje count 8 slotów 2 partycje

// COMMAND ----------

alternateDF.count()

// COMMAND ----------

alternateDF.repartition(8).count()

// COMMAND ----------

// MAGIC %md
// MAGIC **1** Co się stanie jeśli będę miał duży plik z **200 partycjami** i **256 slotów**?
// MAGIC 
// MAGIC **2** Co jeśli będę miał bardzo duży plik **200 partycji** i będę miał tylko **8 slotów**, jak długo potrwa ładowanie w porównianiu z datasetem który ma tylko 8 partycji?
// MAGIC 
// MAGIC **2** Jakie mam opcję jeśli mam (**200 partycji** i **8 slotów**) jeśli nie jestem w stanie zwiększyć ilośći slotów?

// COMMAND ----------

// MAGIC %md
// MAGIC ### Użyj każdego Slot/Core
// MAGIC 
// MAGIC Poza kilkoma wyjątkami staraj się dopasować ilość **partycji do ilośći slotów **.
// MAGIC 
// MAGIC Dzięki temu **wszystkie sloty zostaną użyte** i każdy będzie miał przypisany **task**.
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC Mając 5 partycji i 8 slotów **3 sloty nie będą użyte**.
// MAGIC 
// MAGIC Mając 9 partycji i 8 slotów **job zajmię 2x więcej czasu**.
// MAGIC * Np może to zająć 10 sekund, żeby przetwożyć pierwszych 8  a potem kolejne 10 sekund na ostatnią partycję = 20s.

// COMMAND ----------

// MAGIC %md
// MAGIC Zatem u mnie mając 2 partycje i 8 slotów, 6 slotów nie będzie używanych. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ile Partycji?
// MAGIC 
// MAGIC Podstawowa wartość sugerowana to **200MB na partycję (cached)**.
// MAGIC * Nie patrz na rozmiar na dysku: CSV zajmuje dużo miejsca na dysku ale mniej w RAM: String "12345" = 10B, Integer 12345=4B.
// MAGIC * Parquet skompresowane na dysku ale nie w RAM.
// MAGIC * Relacyjne bazy i inne źródła .....?
// MAGIC 
// MAGIC Wartość **200** pochodzi z doświadczeń Databricks oparty na wydajności. 
// MAGIC 
// MAGIC Jeśli masz wykonawce o niższym RAM (np JVMs with 6GB) możesz  obniżyć tą wartość.
// MAGIC 
// MAGIC Ile RAM Np 8 partycji * 200MB = 1.6GB
// MAGIC 
// MAGIC 
// MAGIC **Pytanie:** Jeśli moje dane będą miały 10 partycji co powinien zrobić ?...
// MAGIC * zredukować ilość partycji (1x ilość slotów)
// MAGIC * czy zwiększyć (2x ilość slotów)
// MAGIC 
// MAGIC **Odpowiedź** To zależy od ilości danych w partycji
// MAGIC * Wczytaj dane. 
// MAGIC * Cache.
// MAGIC * Sprawdź wielkość partycji.
// MAGIC * Jeśli jest powyżej > 200MB to rozważ zwiększenie ilośći partycji.
// MAGIC * Jeśli jest poniżej < 200MB to możesz zmiejszyć ilość partycji.
// MAGIC 
// MAGIC **Celem jest użycie jak najmniejszej liczby partycji i utrzymanie poziomu slotów (przynajmniej 1 x partycji)**.

// COMMAND ----------

// MAGIC %md
// MAGIC ## `coalesce()` i `repartition()`
// MAGIC 
// MAGIC 
// MAGIC **`coalesce(n)`** :
// MAGIC > Returns a new Dataset that has exactly numPartitions partitions, when fewer partitions are requested.<br/>
// MAGIC > If a larger number of partitions is requested, it will stay at the current number of partitions.
// MAGIC 
// MAGIC **`repartition(n)`** :
// MAGIC > Returns a new Dataset that has exactly numPartitions partitions.
// MAGIC 
// MAGIC Różnice
// MAGIC * `coalesce(n)` transformacja **narrow** zmiejsza ilość partycji.
// MAGIC * `repartition(n)` transformacja **wide** może być użyta do zmiejszenia lub zwiększenia ilośći partycji.
// MAGIC 
// MAGIC 
// MAGIC Kiedy użyć jednej lub drugiej.
// MAGIC * `coalesce(n)` nie wywoła shuffle.
// MAGIC * `coalesce(n)` nie gwarantuje równej dystrybujci rekordów na wszystkich partycjach. Może się skończyć z partycjami zawierającymi 80% danych.
// MAGIC * `repartition(n)` jako transformacja **wide** doda koszt shuffle
// MAGIC * `repartition(n)` będzie miało relatywnie równą dystrybujcę danych w partycjach.

// COMMAND ----------


val repartitionedDF = alternateDF.repartition(10)

printf("Partitions: %d%n%n", repartitionedDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Cache
// MAGIC 
// MAGIC Back to list...
// MAGIC 0. Cache the data
// MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
// MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
// MAGIC 0. Possibly re-cache the data if the ETL was costly
// MAGIC 
// MAGIC We just balanced the number of partitions to the number of slots.
// MAGIC 
// MAGIC Depending on the size of the data and the number of partitions, the shuffle operation can be fairly expensive (though necessary).
// MAGIC 
// MAGIC Let's cache the result of the `repartition(n)` call..
// MAGIC * Or more specifically, let's mark it for caching.
// MAGIC * The actual cache will occur later once an action is performed
// MAGIC * Or you could just execute a count to force materialization of the cache.

// COMMAND ----------


repartitionedDF.cache()


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##spark.sql.shuffle.partitions
// MAGIC 
// MAGIC 
// MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
// MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
// MAGIC 0. Possibly re-cache the data if the ETL was costly
// MAGIC 
// MAGIC The next problem has to do with a side effect of certain **wide** transformations.
// MAGIC 
// MAGIC So far, we haven't hit any **wide** transformations other than `repartition(n)`
// MAGIC * But eventually we will... 
// MAGIC * Let's illustrate the problem that we will **eventually** hit
// MAGIC * We can do this by simply sorting our data.

// COMMAND ----------


repartitionedDF
  .orderBy($"category", $"job") // sortuje dane
  .foreach(x => ())               // nie robi nic poza wywołaniem joba

// COMMAND ----------

// MAGIC %md
// MAGIC ### Problem
// MAGIC 
// MAGIC * Jedna akcja.
// MAGIC * Spark wykonał 3 zadania(jobs).
// MAGIC * Sprawdź plan wykonania.
// MAGIC * **Exchange rangepartitioning**
// MAGIC   

// COMMAND ----------


// Look at the explain with all records.
repartitionedDF
  .orderBy($"category", $"job")
  .explain()

println("-"*80)

// Look at the explain with only 3M records.
repartitionedDF
  .orderBy($"category", $"job")
  .limit(3000000)
  .explain()

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Dodatkowe zadania (job) zostały wywołane ilością danych w DataFrame

// COMMAND ----------


repartitionedDF
  .orderBy($"category", $"job") 
  .limit(3000000)                 
  .count()               

// COMMAND ----------

// MAGIC %md
// MAGIC Only 1 job.
// MAGIC 
// MAGIC Spark's Catalyst Optimizer is optimizing our jobs for us!

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### Kolejny Problem
// MAGIC 
// MAGIC * Uruchom orginalny dataframe.
// MAGIC * Przejrzyj wszystkie zadania.
// MAGIC * Sprawdź ile jest partycji w ostatnim jobies!

// COMMAND ----------


val funkyDF = repartitionedDF
  .orderBy($"category", $"job") // sorts the data
                                  //
funkyDF.foreach(x => ())          // litterally does nothing except trigger a job

// COMMAND ----------

// MAGIC %md
// MAGIC Jest aż 200 partycji.

// COMMAND ----------

// MAGIC %md
// MAGIC U mnie 1 partycja.

// COMMAND ----------


printf("Partitions: %,d%n", funkyDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Wartość 200 jest domyślną i opartą na doświadczeniu, pasuje do większości scenariuszy.
// MAGIC 
// MAGIC Moźesz to zmienić w konfiguracji `spark.sql.shuffle.partitions`

// COMMAND ----------


spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

// MAGIC %md
// MAGIC Zmień na 8

// COMMAND ----------


spark.conf.set("spark.sql.shuffle.partitions", "8")

// COMMAND ----------

// MAGIC %md
// MAGIC Ponowne wykonanie dla porównania.

// COMMAND ----------


val betterDF = repartitionedDF
  .orderBy($"category", $"job") // sort the data
                                  
betterDF.foreach(x => () )        // litterally does nothing except trigger a job

printf("Partitions: %,d%n", betterDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md też 1 partycja

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Initial ETL
// MAGIC 
// MAGIC Kilka zmianw procesie ETL żeby wywołać zmiany. 
// MAGIC 
// MAGIC Zmiana Stringa `timestamp` na date time.

// COMMAND ----------


//val pageviewsDF = repartitionedDF
//  .select(
 //   unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp").as("createdAt"), 
  //  $"site", 
   // $"requests"
  //)

val df = repartitionedDF
  .select(
    $"ordering".as("ord"), 
    $"category", 
    $"job"
  )

println("****BEFORE****")
repartitionedDF.printSchema()

println("****AFTER****")
df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Końcowy wynik dla wglądu ile  `DataFrame`

// COMMAND ----------


// mark it as cached.
df.cache() 

// materialize the cache.
df.count() 

// COMMAND ----------

// MAGIC %md
// MAGIC są 2 partycje 
