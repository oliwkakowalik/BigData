// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val filePath = "dbfs:/FileStore/tables/dataLab1/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val namesDf1 = namesDf.withColumn("epoch", unix_timestamp())

val namesDf2 = namesDf1.withColumn("height_feet", col("height")/30.48)

val namesDf3 = namesDf2.withColumn("only_name", substring_index(col("name"), " ", 1))
val name = namesDf3.groupBy("only_name").count().orderBy(desc("count")).limit(1)
//John

val namesDf4 = namesDf3.withColumn("age", months_between(col("date_of_death"), col("date_of_birth"), true).divide(12))

val namesDf5 = namesDf4.drop("bio", "death_details")

val temp = namesDf5.columns.map(x => x.split('_').map(_.capitalize).mkString(""))
val namesDf6= namesDf5.toDF( temp: _*)

val namesDf7 = namesDf5.orderBy(asc("only_name"))

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/dataLab1/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val moviesDf1 = moviesDf.withColumn("epoch", unix_timestamp())

val moviesDf2 = moviesDf1.withColumn("yearsSince", year(current_date())-$"year")

val moviesDf3 = moviesDf2.withColumn("budgetNumeric", substring_index(col("budget"), " ", -1))

val moviesDf4 = moviesDf3.na.drop()

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField, ArrayType}

val filePath = "dbfs:/FileStore/tables/dataLab1/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val ratingsDf1 = ratingsDf.withColumn("epoch", unix_timestamp())

val ratingsDf2 = ratingsDf1.na.drop()

////////////////////////////////////////

val ratingsDf4 = ratingsDf2.select("females_allages_avg_vote","males_allages_avg_vote")
        .agg(avg("females_allages_avg_vote") as "female",avg("males_allages_avg_vote") as "male")

//display(ratingsDf4)
//women give higher votes

import org.apache.spark.sql.types._
val ratingsDf5 = ratingsDf2.withColumn("votes_1",col("votes_1").cast(LongType))

// COMMAND ----------

//zadanie 2

//Jobs Tab 
//Wyświetla stronę wszytskich job'ów w aplikacji Spark'a i strony z detalami dla każdego z nich. Strona podsumowująca wyświetla informacje wysokiego poziomu takie jak np. status, czas trwania, postępy wszytskich job'ów i ogólny timeline wydarzeń. Kliknięcie na konkretnego job'a powoduje przejście do strony z jego detalami, która przedstawia oś czasu, DAG visualization, i wszytskie etapy job'a.

//Stages Tab + Stage detail
//Wyświetla podsumowującą stronę, która pokazuje bieżące stan wszytskich etaapów wszystkich job'ów w aplickacji Spark'a. Na górze strony jest podsumowanie ze zliczeniem stapów względem statusu. Później są szczegóły zdarzeń względem stausu. Można zabić aktywnen zdarzenia używając przeznaczonego do tego linku. W przypadku nieudanych zdarzeń pokazana jest przyczyna niepowodzenia. Szczególy task'a można otworzyć klikając na jego opis. Stage detail zawiera podsumowanie wszystkich task'ów w tabeli i na osi czasu.

//Storage Tab
//wyświetla RDDs i DataFrames. Wyróżnia się “summary page” i “details page”, które dostarczają podstawowe informacje na temat poziomu pamięci, liczby partycji i narzutu pamięci.

//Environment Tab 
//Wyświetla  wartości dla różnych środowisk i konfiguracji zmiennych, w tym JVM, Spark'a i ustawienia systemowe. Ta strona skłąda się z 5 częsci. Jest to odpowiednie miejsce do sprawdzenia czy właściwości zostały poprawnie ustawione. W pierwszej częsci można sprawdzić informacje takie jak np. wersja Javy czy Scali. Druga listuje właściowiści Spark'a. Trzecia właściwości Hadoop'a i YARN'a. Czwarta pokazuje więcej szczegołów JVM. Ostatnia listuje klasy załadowane z innych źródeł co pozwala w rozwiązywaniu konfilktów klas.

//Executors Tab
//Dostarcza podsumowanie o executorach (m.in. wykorzystanie pamięci/dysku/rdzeni, informacje o zadaniach/losowaniu, wydajność). Za pomocą poniższych elementów można wyświelić:
//- łącze 'stderr' executora 0 → standardowy dziennik błędów,
//- link „Thread Dump” executora 0 →  zrzut wątku JVM na executorze 0

//SQL Tab
//Jeżeli aplikacja obsługuje zapytania Spark SQL, SQL Tab wyświetla informacje takie jak czas trwania, job'y, fizyczne i logiczne plany zapytań. Operatory dataframe/SQL sa wylistowane, po naciśnieciu odpowiedniego linku można zobaczyć DAG i szczegóły wykonania zapytania.
//Strona szczegółów zapytania wyświetla informacje na temat czasu wykonania zapytania, jego trwania, listę powiązanych job'ów i DAG wykoniania zapytania.

//Structured Streaming Tab
//Wyświetla zwięzłe statystki dla zapytań (tych skończonych oraz w trakcie). Można sprawdzić ostatni wyjątek jaki został wyrzucony przez zapytanie, które się nie powiodło. Strona statystyk zawiera metryki, które pozwalają na sprawdzenie statusu zapytania (np. czas trwania operacji, wiersze wejściowe)

//Streaming (DStreams) Tab
//Wyświetla zwięzłe statystki dla zapytań (tych skończonych oraz w trakcie). Można sprawdzić ostatni wyjątek jaki został wyrzucony przez zapytanie, które się nie powiodło.

///JDBC/ODBC Server Tab
//Widoczna podczas działania Sparka. Zawiera informacje o sesjach i przesłanych operacjach SQL. Wyróżnia się 3 sekcje:
//-ogólne informacje o serwerze JDBC/ODBC,
//-informacje o aktywnych i zakończonych sesjach,
//-statystyki SQL przesłanych operacji.

// COMMAND ----------

//zadanie 3
val MoviesDf2 = moviesDf.select($"title",$"year").explain()

//Dodanie operacji groupby i explain
val MoviesDf3 = moviesDf.select($"title",$"year").groupBy("year").count().explain()

// COMMAND ----------

//zadanie 4
val username = "sqladmin"
val password = "$3bFHs56&o123$" 

val data = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT table_name FROM information_schema.tables) tmp")
      .option("user", username)
      .option("password",password)
      .load()

display(data)
