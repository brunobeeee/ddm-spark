package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

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

    // Durchlaufe jedes DataFrame in der Liste
    for ((df, index) <- dataFrames.zipWithIndex) {
      val tableName = s"Table_$index"

      // Erstelle ein Dataset mit eindeutigen Werten aus jeder Spalte des DataFrames
      val uniqueValues = df.columns.flatMap { colName =>
        df.select(colName).distinct().withColumn("TableName", lit(tableName)).withColumn("ColumnName", lit(colName))
      }
      uniqueValues.foreach(d => d.foreach(dd => println(dd)))

      // Verbinde die einzelnen Datasets zu einem einzigen Dataset
      val allColumns = uniqueValues.reduce(_ union _)
      allColumns.show()

      // Gruppiere nach Spaltenwerten und zähle die Anzahl der verschiedenen TableName-Werte für jede Spalte
      // val indCandidates = allColumns.groupBy(allColumns.columns.map(col): _*).agg(countDistinct("TableName").alias("DistinctTables"))

      // Filtere die potenziellen INDs heraus, bei denen DistinctTables gleich der Anzahl der Tabellen ist
      // val inds = indCandidates.filter($"DistinctTables" === dataFrames.length)

      // Zeige die gefundenen INDs an
      //println(s"INDs in $tableName:")
      //inds.columns.filter(_ != "DistinctTables").foreach { colName =>
      //  val dependentCols = inds.filter(inds(colName) > 0).select("DistinctTables").as[Long].collect().mkString(", ")
      //  println(s"$colName < $dependentCols")
      //}
    }
  }
}
