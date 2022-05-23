// Databricks notebook source
//Użycie funkcji dropFields i danych Nested.json
Podpowiedź:  withColumn(„nowacol”, $”konlj”.dropfiels(

//2a
//Sprawdź, jak działa ‘foldLeft’ i przerób kilka przykładów z internetu.


// COMMAND ----------

val nestedJson = spark.read.format("json")
                         .option("inferSchema", "true")
                         .option("multiLine", "true")
                         .load("/FileStore/tables/Nested.json")
display(nestedJson)

// COMMAND ----------

display(nestedJson.withColumn("nowacol", $"pathLinkInfo".dropFields("alternateName", "elevationInDirection")))

// COMMAND ----------

// MAGIC %md
// MAGIC Zaczynając od początkowej wartości 0, funkcja foldLeft stosuje funkcję (m, n) => m + n na każdym elemencie listy oraz poprzedniej skumulowanej wartości.

// COMMAND ----------

val nums = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val res = nums.foldLeft(0)((m, n) => m + n)
println(res)

// COMMAND ----------

val numberFunc = numbers.foldLeft(List[Int]())_
val squares = numberFunc((xs, x) => xs:+ x*x)
print(squares.toString())

// COMMAND ----------

case class Person(name: String, sex: String)
val persons = List(Person("Thomas", "male"), Person("Sowell", "male"), Person("Liz", "female"))
val foldedList = persons.foldLeft(List[String]()) { (accumulator, person) =>
  val title = person.sex match {
    case "male" => "Mr."
    case "female" => "Ms."
  }
  accumulator :+ s"$title ${person.name}"
}
assert(foldedList == List("Mr. Thomas", "Mr. Sowell", "Ms. Liz"))
