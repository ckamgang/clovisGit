package snowflake.snowflake
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object scala {def main(args: Array[String]): Unit = {
  Logger.getRootLogger.setLevel(Level.INFO)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkDemo04")
    .getOrCreate()

  val sfOptions = Map(
    "sfURL" -> "https://igb75349.us-east-1.snowflakecomputing.com",
    "sfUser" -> "ckamgang",
    "sfPassword" -> "Soixante12@@",
    "sfDatabase" -> "PRATICE",
    "sfSchema" -> "TEST1",
    "sfWarehouse" -> "COMPUTE_WH",
    "sfRole" -> "accountadmin"
  )

  val df: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "BD_TABLE_01")
   // .option("query", "select id, name, preferences, created_at from big_data_table1")
    .load()

  df.show(false)

}

}
