package com.miguellobato.delta_schema_evolution

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaMergeSpec
  extends AnyFlatSpec
    with Matchers
    with SchemaMerge
    with DataFrameComparer {

  val spark = LocalSpark().init("test")

  val sampleInvalidSchema = new StructType()
    .add(StructField(name = "_p1", dataType = StringType))
    .add(StructField(name = "p2", dataType = new StructType()
      .add(StructField(name = "_p2.1", dataType = StringType))
      .add(StructField(name = "p2.2", dataType = new StructType()
        .add(StructField(name = "p2.2.1", dataType = StringType))
        .add(StructField(name = "p2.2.2", dataType = StringType))
        .add(StructField(name = "p2.2.3-x", dataType = StringType))
      ))))
    .add(StructField(name = "p3-x", dataType = StringType))

  val sampleSchema = new StructType()
    .add(StructField(name = "field_p1", dataType = StringType))
    .add(StructField(name = "p2", dataType = new StructType()
      .add(StructField(name = "field_p2_1", dataType = StringType))
      .add(StructField(name = "p2_2", dataType = new StructType()
        .add(StructField(name = "p2_2_1", dataType = StringType))
        .add(StructField(name = "p2_2_2", dataType = StringType))
        .add(StructField(name = "p2_2_3_x", dataType = StringType))
      ))))
    .add(StructField(name = "p3_x", dataType = StringType))

  val sampleData = Seq(
    Row("v1=_p1", Row("v1=_p2.1", Row("v1=p2.2.1", "v1=p2.2.2", "v1=p2.2.3-x")), "v1=p3-x"),
    Row("v2=_p1", Row("v2=_p2.1", Row("v2=p2.2.1", "v2=p2.2.2", "v2=p2.2.3-x")), "v2=p3-x")
  )

  it should "apply rename transformation" in {
    val sourceDF = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), sampleInvalidSchema)
      .transform(rename())

    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), sampleSchema)
    assertSmallDataFrameEquality(sourceDF, expectedDF)
  }

  it should "evolute schema from int to string in level1" in {
    val sourceschema = new StructType()
      .add(StructField(name = "field_p1", dataType = IntegerType))

    val expectedschema = new StructType()
      .add(StructField(name = "field_p1", dataType = StringType))
      .add(StructField(name = COLUMN_WARNING, dataType = StringType))

    val sourceDF = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1))), sourceschema)
      .transform(evolve(expectedschema))
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("1", "field_p1:IntegerType:StringType"))), expectedschema)

    assertSmallDataFrameEquality(sourceDF, expectedDF, ignoreNullable=true)
  }

  it should "evolute schema from int to string in level2" in {}

  it should "evolute schema from int to string in level3" in {}

  it should "evolute schema from struct to string in level1" in {}

  it should "evolute schema from struct to string in level2" in {}

  it should "evolute schema from struct to string in level3" in {}

  it should "evolute schema from array to string in level1" in {}

  it should "evolute schema from array to string in level2" in {}

  it should "evolute schema from array to string in level3" in {}

  it should "evolute schema from array to struct in level1" in {}

  it should "evolute schema from array to struct in level2" in {}

  it should "evolute schema from array to struct in level3" in {}

  it should "evolute schema from struct to array in level1" in {}

  it should "evolute schema from struct to array in level2" in {}

  it should "evolute schema from struct to array in level3" in {}

  it should "evolute schema from string to int in level1" in {}

  it should "evolute schema from string to int in level2" in {}

  it should "evolute schema from string to int in level3" in {}

  it should "add new properties in level 1" in {}

  it should "add new properties in level 2" in {}

  it should "add new properties in level 3" in {}

  it should "ignore non existing properties in level 1" in {}

  it should "ignore non existing properties in level 2" in {}

  it should "ignore non existing properties in level 3" in {}
}
