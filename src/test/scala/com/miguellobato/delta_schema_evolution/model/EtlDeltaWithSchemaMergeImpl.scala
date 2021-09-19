package com.miguellobato.delta_schema_evolution.model

import com.miguellobato.delta_schema_evolution.EtlDeltaWithSchemaMerge
import org.apache.spark.sql.types.StructType

case class EtlDeltaWithSchemaMergeImpl()
  extends EtlDeltaWithSchemaMerge {

  /**
   *
   * @param targetSchema
   * @param sourceSchema
   * @return
   * _1: StructType => the new struct that need to be used while reading
   * _2: Boolean    => indicates if there is changes in the struct and table needs to be recreated
   */
  override def mergeSchema(targetSchema: StructType, sourceSchema: StructType): (StructType, Boolean) = super.mergeSchema(targetSchema, sourceSchema)
}
