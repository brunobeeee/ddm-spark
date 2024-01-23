package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}


object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // Lese die Daten für alle Tabellen ein
    val dataFrames = inputs.map(input => readData(input, spark))

    // Sammle alle Tupel in einer Seq
    var allTuples: Seq[(String, String)] = Seq()

    for (df <- dataFrames) {
      val columnNames = df.columns

      val uniqueTuples = df.columns.flatMap { colName =>
        df.select(colName).distinct().collect().map(row => (row(0).toString, colName))
      }

      // Sammle alle Tupel
      allTuples = allTuples ++ uniqueTuples
    }

    // Reduziere die Tupel und vereinige die Attribute
    val reducedTuplesDF = allTuples.toDF("Value", "ColumnName")
      .groupBy("Value")
      .agg(collect_set("ColumnName").as("Attributes"))
      .orderBy("Value")

    reducedTuplesDF.show(truncate = false)

    

  }
}
