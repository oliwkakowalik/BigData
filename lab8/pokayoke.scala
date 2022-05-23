// Databricks notebook source
import org.apache.spark.sql.functions.udf

// COMMAND ----------

val pCase = (s: pCase) =>
{
  if(s.count)
   s.split("_").map(_.capitalize).mkString("")
  else
  ""
}

spark.udf.register("pCase", pCase)

// COMMAND ----------

val sqrt_d = (a: Double ) =>
{
  import scala.math.sqrt
  if (a<0) 0 else sqrt(a)
}
spark.udf.register("sqrt_d", sqrt_d)

// COMMAND ----------

val mean = (s: List[Integer]) =>
{
 if (s.count == 0) 
    0
 else s.sum/s.count
}
spark.udf.register("mean", mean)

// COMMAND ----------

def fillNan(colname: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(colname)){
     return df.na.fill(0,Array(colname))
   }
  else
  {
      Logger.getLogger("column does not exist in df").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }

}
