package com.miguellobato.delta_schema_evolution

import com.miguellobato.delta_schema_evolution.model.EtlDeltaWithSchemaMergeImpl
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeltaSchemaEvolutionSpec extends
  AnyFlatSpec
  with Matchers {

  "DeltaSchemaEvolution" should "handle schema evolution" in {
    val spark = LocalSpark().init("test")

    val sourceSchema = new StructType()
      .add(StructField(name = "source", dataType = StringType))

    val targetSchema = new StructType()
      .add(StructField(name = "source", dataType = StringType))

    val expectedSchema = new StructType()
      .add(StructField(name = "source", dataType = StringType))

    EtlDeltaWithSchemaMergeImpl().mergeSchema(sourceSchema, targetSchema) should be(expectedSchema, false)
  }
}
