package com.miguellobato.delta_schema_evolution

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SchemaManagement {

  private val logger = Logger(this.getClass.getName)
  val COLNAME_DATAOPS_CORRUPT_RECORD = "dataops_corrupt_record"

  def prepareFullSchema(schema: StructType, include_errors_col: Boolean): StructType = {
    def _prepareFullSchema(structField: StructField): StructField = {
      structField.dataType match {
        case p: StructType => StructField(name = structField.name, dataType = StructType(fields = p.fields.map(t => _prepareFullSchema(t))))
        case p: ArrayType  => StructField(name = structField.name, dataType = ArrayType(elementType = StringType, containsNull = true))
        case _: MapType    => StructField(name = structField.name, dataType = MapType(keyType = StringType, valueType = StringType))
        case _             => structField
      }
    }

    val extra = if (include_errors_col) Seq(StructField(COLNAME_DATAOPS_CORRUPT_RECORD, StringType, false)) else Seq.empty[StructField]
    new StructType(fields = schema.fields.map(t => _prepareFullSchema(t)) ++ extra)
  }

  def mergeschema(targetSchema: StructType, sourceschema: StructType): StructType = {
    def _mergefields(target: Option[StructField], source: Option[StructField]): StructField = {
      /*
      1) solo en target o solo en source => return
      2) en ambos, pero son iguales => target
      3) en ambos, pero target es basic => target
      4) en ambos, pero ambos son struct => recursive
       */
      if (target.isEmpty || source.isEmpty)
        source.getOrElse(target.get)
      else if (target.get.dataType.json == source.get.dataType.json)
        target.get
      else if (source.get.dataType.simpleString.startsWith("struct") && target.get.dataType.simpleString.startsWith("struct"))
        StructField(
          name = target.get.name,
          dataType = mergeschema(targetSchema = target.get.dataType.asInstanceOf[StructType], sourceschema = source.get.dataType.asInstanceOf[StructType])
        )
      else if (source.get.dataType == StringType && target.get.dataType.simpleString.startsWith("array"))
        throw new Exception(s"Cannot merge schema for field ${source.get.name}: source=${source.get.dataType.simpleString}, target=${target.get.dataType.simpleString}")
      else
        target.get
    }

    val all = sourceschema.map(t => t.name).union(targetSchema.map(t => t.name)).distinct
    StructType(fields =
      all.map(t =>
        _mergefields(
          target = targetSchema.find(x => x.name == t),
          source = sourceschema.find(x => x.name == t)
        )
      )
    )
  }
}