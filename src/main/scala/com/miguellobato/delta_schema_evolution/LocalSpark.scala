package com.miguellobato.delta_schema_evolution

import org.apache.spark.sql.SparkSession

case class LocalSpark() {

  private val defaultURI = "mongodb://localhost:27017/test.article"
  private val testFolder = "file://tmp/spark_test"

  def init(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.mongodb.input.uri", defaultURI)
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .config("spark.databricks.delta.optimizeWrite.enabled", "true")
      .config("spark.sql.warehouse.dir", s"${testFolder}/warehouse")
      .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=${testFolder}/derby")
      .config("spark.sql.caseSensitive", "true")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

}
