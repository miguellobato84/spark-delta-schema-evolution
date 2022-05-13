package com.miguellobato.delta_schema_evolution

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeltaSchemaEvolutionSpec
  extends AnyFlatSpec
    with Matchers {

  "DeltaSchemaEvolution" should "handle no schema evolution" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, false)
  }

  it should "add a new field" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "source", dataType = StringType))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    val expectedSchema = new StructType()
      .add(StructField(name = "source", dataType = StringType))
      .add(StructField(name = "target", dataType = StringType))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, true)
  }

  it should "add a new field in struct" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = StructType(Seq(
        StructField(name = "source_field", dataType = StringType)
      ))))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = StructType(Seq(
        StructField(name = "target_field", dataType = StringType)
      ))))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = StructType(Seq(
        StructField(name = "source_field", dataType = StringType),
        StructField(name = "target_field", dataType = StringType)
      ))))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, true)
  }

  it should "add a new field in array of struct" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(elementType = StructType(Seq(
        StructField(name = "source_field", dataType = StringType)
      )))))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(elementType = StructType(Seq(
        StructField(name = "target_field", dataType = StringType)
      )))))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(elementType = StructType(Seq(
        StructField(name = "source_field", dataType = StringType),
        StructField(name = "target_field", dataType = StringType)
      )))))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, true)
  }

  it should "handle fallback cast to string" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = LongType))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, false)
  }

  it should "handle fallback cast to string (in array)" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(LongType)))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(StringType)))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(StringType)))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, false)
  }

  it should "respect the original type" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = StringType))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = LongType))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = LongType))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, false)
  }

  it should "respect the original type (in array)" in {
    val sourceSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(StringType)))

    val targetSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(LongType)))

    val expectedSchema = new StructType()
      .add(StructField(name = "target", dataType = ArrayType(LongType)))

    SchemaManagement.mergeschema(targetSchema, sourceSchema) should be(expectedSchema, false)
  }
}
