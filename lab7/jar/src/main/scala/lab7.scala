import org.apache.spark.sql.SparkSession
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.functions.{col, expr}

import java.net.URL

object lab7 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val data = new URL("https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv")
    val file = IOUtils.toString(data,"UTF-8").lines.toList.toDS()

    val df = spark.read.option("header", value = true).option("inferSchema", value = true).csv(file)

    val df_2 = df
      .withColumn("VarietyShort", expr("substring(variety, 1, 2)"))
      .withColumn("SepalArea", col("`sepal.length`") * col("`sepal.width`"))
  }
}
