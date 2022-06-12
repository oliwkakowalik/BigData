package org
import org.transformations.Filter
import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.DataFrame
import org.caseClass.Iris
import org.data.{DataReader, DataWriter}


object App{

  def main(args: Array[String]) : Unit =
  {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Maven_first_app")
      .getOrCreate();

    import spark.implicits._
    val reader= new DataReader();

    val iris_df:DataFrame=reader.read_csv("C:\\Users\\hp\\Documents\\Studia\\3 rok\\6 semestr\\big_data\\lab11_oliwia\\iris.csv", spark.sqlContext, header = true );
    iris_df.show(10);

    val iris_dataset= iris_df.as[Iris];
    val filter = new Filter();

    val filtered = iris_dataset.filter(row => filter.sepal_w_gt(row, 2.3))

    val writer=new data.DataWriter();
    writer.write(filtered.toDF(),"C:\\Users\\hp\\Documents\\Studia\\3 rok\\6 semestr\\big_data\\lab11_oliwia\\summary.csv");


  }




}
