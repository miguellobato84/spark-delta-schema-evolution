# Test-case-1
* **Source** (old schema): STRING
* **Target** (new schema): STRUCT
* **Result**: New field will be stored as json in a string type

The execution log for the built-in schema merge is as follows:
```
Source schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

Target schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: struct (nullable = true)
 |    |    |-- a4: string (nullable = true)
 |    |    |-- a5: string (nullable = true)
 |    |-- a4: string (nullable = true)

org.apache.spark.sql.AnalysisException: Failed to merge fields 'a2' and 'a2'. Failed to merge fields 'a3' and 'a3'. Failed to merge incompatible data types StringType and StructType(StructField(a4,StringType,true), StructField(a5,StringType,true))
```

The execution log with our custom function is as follows:
```
Schema evolution - FORCE CAST a1 (StringType) -> StringType
Schema evolution - RECURSIVE a2
  Schema evolution - FORCE CAST a3 (StructType(StructField(a4,StringType,true), StructField(a5,StringType,true))) -> StringType
  Schema evolution - FORCE CAST a4 (StringType) -> StringType
  
Source schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

New schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: struct (nullable = true)
 |    |    |-- a4: string (nullable = true)
 |    |    |-- a5: string (nullable = true)
 |    |-- a4: string (nullable = true)

Calculated schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

+---+--------------------+
| a1|                  a2|
+---+--------------------+
| v1|{{"a4":"v4","a5":...|
| v1|            {v3, v4}|
+---+--------------------+
```

Yeah, it is casting a struct into a string, but we consider that better than a failed pipeline.

# Test-case-2
* **Source** (old schema): STRUCT
* **Target** (new schema): STRING
* **Result**: function will throw an exception indicating this cast is not possible

The execution log for the built-in schema merge is as follows:
```
Source schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: struct (nullable = true)
 |    |    |-- a4: string (nullable = true)
 |    |    |-- a5: string (nullable = true)
 |    |-- a4: string (nullable = true)

New schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

org.apache.spark.sql.AnalysisException: Failed to merge fields 'a2' and 'a2'. Failed to merge fields 'a3' and 'a3'. Failed to merge incompatible data types StructType(StructField(a4,StringType,true), StructField(a5,StringType,true)) and StringType
```

The execution log with our custom function is as follows:
```
Schema evolution - FORCE CAST a1 (StringType) -> StringType
Schema evolution - RECURSIVE a2
  Schema evolution - FORCE CAST a3 (StringType) -> StructType(StructField(a4,StringType,true), StructField(a5,StringType,true))
java.lang.Exception: Unable to perform this type of cast
```

# Test-case-3
* **Source** (old schema): STRING
* **Target** (new schema): NUMBER
* **Result**: New field will be stored as string type
The execution log for the built-in schema merge is as follows:
```
Source schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

New schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: long (nullable = true)
 |    |-- a4: string (nullable = true)

org.apache.spark.sql.AnalysisException: Failed to merge fields 'a2' and 'a2'. Failed to merge fields 'a3' and 'a3'. Failed to merge incompatible data types StringType and LongType
```

The execution log with our custom function is as follows:
```
Schema evolution - FORCE CAST a1 (StringType) -> StringType
Schema evolution - RECURSIVE a2
  Schema evolution - FORCE CAST a3 (LongType) -> StringType
  Schema evolution - FORCE CAST a4 (StringType) -> StringType

Source schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

New schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: long (nullable = true)
 |    |-- a4: string (nullable = true)

Calculated schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

+---+--------+
| a1|      a2|
+---+--------+
| v1|{v3, v4}|
| v1| {1, v4}|
+---+--------+
```

# Test-case-4
* **Source** (old schema): STRING
* **Target** (new schema): NUMBER
* **Result**: function will throw an exception indicating this cast is not possible. An evolution of this could be map the new function to another column

The execution log for the built-in schema merge is as follows:
```
Source schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: long (nullable = true)
 |    |-- a4: string (nullable = true)

New schema
root
 |-- a1: string (nullable = true)
 |-- a2: struct (nullable = true)
 |    |-- a3: string (nullable = true)
 |    |-- a4: string (nullable = true)

org.apache.spark.sql.AnalysisException: Failed to merge fields 'a2' and 'a2'. Failed to merge fields 'a3' and 'a3'. Failed to merge incompatible data types LongType and StringType
```

The execution log with our custom function is as follows:
```
Schema evolution - FORCE CAST a1 (StringType) -> StringType
Schema evolution - RECURSIVE a2
  Schema evolution - FORCE CAST a3 (StringType) -> LongType
java.lang.Exception: Unable to perform this type of cast
```

# Test-case-5
In this case, some new fields are added. Delta will perform this evolution in its own, but we need to take care in our function also.