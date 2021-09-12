import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.{DataType, StructField, StructType, StringType}

val sourcepath = "source.json"
val targetpath = "target.json"
val deltapath = "delta"

  def schemaEvolution(oldSchema: StructType, newSchema: StructType, level: Int = 0): StructType = {

    val tab = List.fill(level)("  ").mkString
    val result = new StructType()

    //Add new cols
    val newSchemaNewCols = newSchema.names.diff(oldSchema.names).map(t => newSchema.find(x => x.name == t).get)
    newSchemaNewCols.foreach(result.add(_))
    newSchemaNewCols.foreach(t => println(s"${tab}Schema evolution - NEW ${t.name}"))

    for (oldCol <- oldSchema.fields) {
      val newColStruct = newSchema.find(t => t.name == oldCol.name)
      if (newColStruct.isEmpty) {
        println(s"${tab}Schema evolution - OLD ${oldCol.name}")
        result.add(oldCol)
      } else {
        val newCol = newColStruct.get
        if (oldCol.dataType.isInstanceOf[StructType] && newCol.dataType.isInstanceOf[StructType]) {
          println(s"${tab}Schema evolution - RECURSIVE ${oldCol.name}")
          val newSchema = schemaEvolution(oldCol.dataType.asInstanceOf[StructType], newCol.dataType.asInstanceOf[StructType], level + 1)
          result.add(StructField(name = oldCol.name, dataType = newSchema.asInstanceOf[DataType]))
        } else if (oldCol.dataType.isInstanceOf[StringType]) {
          println(s"${tab}Schema evolution - FORCE CAST ${newCol.name} (${newCol.dataType}) -> ${oldCol.dataType}")
          result.add(oldCol)
        } else {
          println(s"${tab}Schema evolution - FORCE CAST ${newCol.name} (${newCol.dataType}) -> ${oldCol.dataType}")
          throw new Exception("Unable to perform this type of cast")
        }
      }
    }

    oldSchema
  }

//Read schemas and custom evolution
val oldSchema = spark.read.json(sourcepath).limit(0).schema
val newSchema = spark.read.json(targetpath).limit(0).schema
val targetSchema = schemaEvolution(oldSchema, newSchema)

println("Source schema")
oldSchema.printTreeString()
println("New schema")
newSchema.printTreeString()
println("Calculated schema")
targetSchema.printTreeString()

//Load source and target DF
val dfsource = spark.read.json(sourcepath)
val dftarget = spark.read.schema(targetSchema).json(targetpath)

//Attempt to write
dfsource.write.format("delta").save(deltapath)
dftarget.write.mode("append").format("delta").save(deltapath)

//Read result written
spark.read.format("delta").load(deltapath).show()