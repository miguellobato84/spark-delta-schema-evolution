import io.delta.tables.DeltaTable

val sourcepath = "source.json"
val targetpath = "target.json"
val deltapath = "delta"


//Read schemas
val oldSchema = spark.read.json(sourcepath).limit(0).schema
val newSchema = spark.read.json(targetpath).limit(0).schema

println("Source schema")
oldSchema.printTreeString()
println("New schema")
newSchema.printTreeString()

//Load source and target DF
val dfsource = spark.read.json(sourcepath)
val dftarget = spark.read.json(targetpath)

//Attempt to write
dfsource.write.format("delta").save(deltapath)
dftarget.write.mode("append").format("delta").save(deltapath)

