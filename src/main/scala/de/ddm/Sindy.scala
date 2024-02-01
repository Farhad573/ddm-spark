package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

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
    // Read data from input files and convert them into Spark Datasets
    val datasets = inputs.map(input => readData(input, spark))
    //    println("Datasets:")
    //    datasets.foreach(println)

    // Flatten datasets into tuples of (value, columnName)
    val flattenedData =
      datasets.map(ds => { // Spark Dataset -> tuples of value and corresponding columnName
        val columns = ds.columns
        ds.flatMap(row => {
          for (i <- columns.indices) yield {
            (row.getString(i), columns(i))
          }
        }) // (value, colName)
      })
    //    println("Flattened Data:")
    //    flattenedData.foreach(println)

    // Merge all datasets into a single dataset
    val mergedData = flattenedData.reduce((ds1, ds2) => ds1.union(ds2))
    //    println("Merged Data:")
    //    mergedData.show()
  }
}
