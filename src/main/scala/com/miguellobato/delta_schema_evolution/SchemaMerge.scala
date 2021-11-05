package com.miguellobato.delta_schema_evolution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, to_json}
import org.apache.spark.sql.types._

trait SchemaMerge {

  val COLUMN_INFO = "column_info"
  val COLUMN_WARNING = "column_warning"

  case class Property(path: Array[String], propertyType: DataType) {

    lazy val isLevel1Property = path.length == 1
    lazy val level1Property = path.head
    lazy val fullpathkey = if (isLevel1Property) path.head else path.map(t => s"`$t`").mkString(".")
    lazy val fullpathrenamed = if (isLevel1Property) _validstr(path.head) else path.map(t => s"`${_validstr(t)}`").mkString(".")
    lazy val selfrefkey = (path.dropRight(1).map(t => s"`${_validstr(t)}`") :+ s"`${path.last}`").mkString(".")
    lazy val droppath = (path.tail.dropRight(1).map(t => s"`${_validstr(t)}`") :+ s"`${path.last}`").mkString(".")
    lazy val subpathrenamed = path.tail.map(t => s"`${_validstr(t)}`").mkString(".")
    lazy val needsRenaming = fullpathkey != fullpathrenamed


    def rename(): DataFrame => DataFrame = df => {
      if (needsRenaming) {
        _printTransformation()
        if (isLevel1Property) {
          df.withColumnRenamed(fullpathkey, _validstr(fullpathrenamed))
        } else {
          df.withColumn(level1Property,
            col(level1Property)
              .withField(subpathrenamed, col(selfrefkey))
              .dropFields(droppath))
            .transform(
              _breakPlan()
            )
        }
      } else {
        df
      }
    }

    def evolve(target: Property): DataFrame => DataFrame = df => {
      def _infoevolution(text: String, colname: String): DataFrame => DataFrame = df => {
        if (df.schema.fields.exists(t => t.name == colname))
          df.withColumn(colname, concat(col(colname), lit(s"; $text")))
        else
          df.withColumn(colname, lit(text))
      }

      if (target.propertyType == propertyType) {
        df
      } else if (target.propertyType == StringType && isStructArrayMap(propertyType)) {
        _printEvolve(target)
        if (isLevel1Property) {
          df.withColumn(level1Property, to_json(col(level1Property))).transform(_infoevolution(getEvolveMessage(target.propertyType), COLUMN_INFO))
        } else {
          throw new NotImplementedError()
        }
      } else {
        _printEvolve(target)
        if (isLevel1Property) {
          df.withColumn(level1Property, col(level1Property).cast(target.propertyType)).transform(_infoevolution(getEvolveMessage(target.propertyType), COLUMN_WARNING))
        } else {
          throw new NotImplementedError()
        }
      }
    }

    def getEvolveMessage(targettype: DataType): String = s"$fullpathrenamed:$propertyType:$targettype"

    private def _breakPlan(): DataFrame => DataFrame = df => df.sparkSession.createDataFrame(df.rdd, schema = df.schema)

    private def isStructArrayMap(propertyType: DataType) = {
      val STRUCT = "struct"
      val MAP = "map"
      val ARRAY = "array"
      propertyType.simpleString.startsWith(STRUCT) || propertyType.simpleString.startsWith(MAP) || propertyType.simpleString.startsWith(ARRAY)
    }

    private def _validstr(x: String): String = {
      //Remove invalid characters
      val chars1 = Array(' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=')
      val step1 = chars1.foldLeft(x)((a, b) => a.replace(b.toString, ""))

      //Replace invalid characters
      val chars2 = Map("." -> "_", "+" -> "plus_", "-1" -> "minus_1", "-" -> "_")
      val step2 = chars2.foldLeft(step1)((a, b) => a.replace(b._1, b._2))

      if (step2.startsWith("_")) "field" + step2 else step2
    }

    override def toString: String = s"$fullpathkey ($propertyType)"

    private def _printTransformation() = {
      if (needsRenaming) {
        println(toString())
        println(s"  needsRenaming=$needsRenaming")
        println(s"  fullpathrenamed=$fullpathrenamed")
        println(s"  selfrefkey=$selfrefkey")
        println(s"  droppath=$droppath")
        println(s"  subpathrenamed=$subpathrenamed")
        if (isLevel1Property) {
          println(s"  rename=${_validstr(fullpathrenamed)} => $fullpathkey")
        } else {
          println(s"  rename=col($level1Property).withField($subpathrenamed, col($selfrefkey)).dropFields($droppath) => $level1Property")
        }
      }
    }

    private def _printEvolve(target: Property): Unit = {
      println(toString())
      println(s"  source=$fullpathrenamed")
      println(s"  target=${target.fullpathrenamed}")
      if (isLevel1Property) {
        println(s"  evolve=${propertyType.simpleString} => ${target.propertyType.simpleString}")
      } else {
        println(s"  rename=col($level1Property).withField($subpathrenamed, col($selfrefkey)).dropFields($droppath) => $level1Property")
      }
    }
  }

  def retrieveAllProperties(schema: StructType): Seq[Property] = {
    def _retrieveAllProperties(schema: StructType, parent: Array[String]): Seq[Property] = {
      val r: Seq[Seq[Property]] = schema.fields.map(t => {
        val array = parent :+ t.name
        t.dataType match {
          case p: StructType => Property(array, p) +: _retrieveAllProperties(p, array)
          case p: ArrayType => if (p.elementType.isInstanceOf[StructType]) {
            Property(array, p) +: _retrieveAllProperties(p.elementType.asInstanceOf[StructType], array)
          } else {
            Seq(Property(array, p))
          }
          case _ => Seq(Property(array, t.dataType))
        }
      })
      r.flatten
    }

    _retrieveAllProperties(schema, parent = Array.empty)
  }

  def rename(): DataFrame => DataFrame = df => {
    val properties = retrieveAllProperties(df.schema).filter(t => t.needsRenaming)
    properties.foldLeft(df) { (tempdf, t) => tempdf.transform(t.rename()) }
  }

  def evolve(targetschema: StructType): DataFrame => DataFrame = df => {
    val sourceproperties = retrieveAllProperties(df.schema)
    val targetproperties = retrieveAllProperties(targetschema)

    val properties = sourceproperties
      .filter(t => targetproperties.exists(x => x.fullpathrenamed == t.fullpathrenamed && x.propertyType != t.propertyType))
      .map(t => (t, targetproperties.find(x => x.fullpathrenamed == t.fullpathrenamed).get))

    properties.foldLeft(df) { (tempdf, t) => tempdf.transform(t._1.evolve(t._2)) }
  }

  def debug(name: String): DataFrame => DataFrame = df => {
    println(name)
    df.show(truncate = false)
    df.printSchema()
    df
  }
}
