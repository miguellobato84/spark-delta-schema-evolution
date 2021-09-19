package com.miguellobato.delta_schema_evolution

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait EtlDeltaWithSchemaMerge {

  /**
   * Retrieves the source schema
   *
   * @param spark
   * @return
   */
  def getSourceSchema(spark: SparkSession): StructType = ???

  /**
   * Retrieves the target schema (current existing in data lake)
   *
   * @param spark
   * @return
   */
  def getTargetSchema(spark: SparkSession): StructType = ???

  /**
   * Read source with a custom schema (as calculated by mergeSchema)
   *
   * @param spark
   * @param schema
   * @return
   */
  def read(spark: SparkSession, schema: StructType): DataFrame = ???

  /**
   * Save to target
   *
   * @param spark
   * @param result
   * @param schemaChanged Notify if schema has changed, so table should be recreated
   */
  def save(spark: SparkSession, result: DataFrame, schemaChanged: Boolean): Unit = ???

  /**
   *
   * @param targetSchema
   * @param sourceSchema
   * @return
   * _1: StructType => the new struct that need to be used while reading
   * _2: Boolean    => indicates if there is changes in the struct and table needs to be recreated
   */
  def mergeSchema(targetSchema: StructType, sourceSchema: StructType): (StructType, Boolean) = {
    val (resultRead, changes) = _getSchemaEvolution(targetSchema, sourceSchema)
    (resultRead, changes)
  }

  /**
   * Calculate the schema evolution to avoid delta lake merge conflicts
   *
   * @param oldSchema target schema
   * @param newSchema source schema
   * @param parentcol for recursive purpose, the name of the struct
   * @return the new schema
   *
   */
  private def _getSchemaEvolution(oldSchema: StructType, newSchema: StructType, level: Int = 0): (StructType, Boolean) = {

    val tab = "  " * level
    var result = new StructType()
    var changes = false

    //Add cols existing only in the new schema
    val newSchemaNewCols = newSchema.names.diff(oldSchema.names).map(t => newSchema.find(x => x.name == t).get)
    newSchemaNewCols.foreach(t => {
      result = result.add(t)
      changes = true
    })
    newSchemaNewCols.foreach(t => println(s"${tab} Schema evolution - NEW ${t.name}"))

    //Traverse cols in the old schema
    for (oldCol <- oldSchema.fields) {
      //find the one with same name in the new schema
      val newColStruct = newSchema.find(t => t.name == oldCol.name)

      if (newColStruct.isEmpty) {
        //if not found, this column is deprecated. Added to the schema for backwards compatibility
        result = result.add(oldCol)
      } else {
        //some helpers
        val newCol = newColStruct.get
        val areBothStruct = oldCol.dataType.isInstanceOf[StructType] && newCol.dataType.isInstanceOf[StructType]
        val areBothArrayStruct = oldCol.dataType.isInstanceOf[ArrayType] &&
          newCol.dataType.isInstanceOf[ArrayType] &&
          oldCol.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType] &&
          newCol.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
        val isArrayString = oldCol.dataType.isInstanceOf[ArrayType] &&
          oldCol.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StringType]

        if (areBothStruct) {
          //oldCol and newCol are StructType, we called this function recursively
          println(s"${tab} Schema evolution - STRUCT ${oldCol.name}")
          val oldStruct = oldCol.dataType.asInstanceOf[StructType]
          val newStruct = newCol.dataType.asInstanceOf[StructType]
          val (recursiveResult, changesResult) = _getSchemaEvolution(oldStruct, newStruct, level + 1)
          result = result.add(StructField(name = oldCol.name, dataType = recursiveResult.asInstanceOf[DataType]))
          changes = changes || changesResult
        }
        else if (areBothArrayStruct) {
          //oldCol and newCol are Array[StructTyppe], we called this function recursively
          println(s"${tab} Schema evolution - ARRAY ${oldCol.name}")
          val oldStruct = oldCol.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val newStruct = newCol.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val (recursiveResult, changesResult) = _getSchemaEvolution(oldStruct, newStruct, level + 1)
          result = result.add(StructField(name = oldCol.name, dataType = ArrayType(elementType = recursiveResult)))
          changes = changes || changesResult
        }
        else if (oldCol.dataType == newCol.dataType) {
          //oldCol and newCol are of the same type (but not StructType or Array[StructTyppe])
          result = result.add(oldCol)
        }
        else if (oldCol.dataType.isInstanceOf[StringType] || isArrayString) {
          //oldCol is StringType or Array[StringType]. We can cast safely to these types
          println(s"${tab} Schema evolution - CAST ${newCol.name} (${newCol.dataType}) -> ${oldCol.dataType}")
          result = result.add(oldCol)
        }
        else {
          //We have a conflict. Type of oldCol is different from newCol and cannot be casted safely.
          //json will try to parse and if not, it will be leaved blank
          println(s"${tab} WARN Schema evolution - CANNOT CAST ${oldCol.name} (${oldCol.dataType}) -> (${newCol.dataType})")
          result = result.add(oldCol)
        }
      }
    }

    (result, changes)
  }
}
