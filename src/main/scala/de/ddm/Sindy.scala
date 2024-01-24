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

  private def generateInclusionLists(attributes: Seq[String]): Seq[(String, Seq[String])] = {
    attributes.flatMap { attribute =>
      val inclusionList = Seq(attribute, null)
      val otherAttributes = attributes.filterNot(_ == attribute)
      inclusionList.map(entry => (attribute, entry +: otherAttributes))
    }
  }

  def transformList(inputList: Seq[String]): Seq[Seq[String]] = {
    inputList match {
      case Seq(value) => Seq(Seq(value, null))
      case list if list.length == 2 => Seq(Seq(list.head, list(1)), Seq(list(1), list.head))
      case list if list.length > 2 =>
        val permutations = list.permutations.toSeq
        permutations.map(permutation => Seq(permutation.head) ++ permutation.tail)
      case _ => Seq.empty
    }
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

    reducedTuplesDF.show()

    // Wende explode() auf die Spalte "Attributes" an
    val explodedDF = reducedTuplesDF.select(explode(col("Attributes")).alias("key"), col("Attributes").alias("value"))

    explodedDF.show()

    // Delete all orrucencies of keys in the values
    val emptySetDF = explodedDF.withColumn("value", array_remove(col("value"), col("key"))).dropDuplicates()

    emptySetDF.show()


    import org.apache.spark.sql.expressions.Window

    val smallestListDF = emptySetDF
      .withColumn("min_size", min(size(col("value"))).over(Window.partitionBy("key")))
      .filter(col("min_size") === size(col("value")))
      .drop("min_size")

    smallestListDF.show()

    val finalDF = smallestListDF.filter(size(col("value")) > 0)

    finalDF.show()

  }
}
