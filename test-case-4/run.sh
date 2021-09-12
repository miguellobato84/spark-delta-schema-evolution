rm -rf delta
echo :quit | spark-shell --packages io.delta:delta-core_2.12:1.0.0 --conf spark.databricks.delta.schema.autoMerge.enabled=true -i script-without-schema-evolution.scala

rm -rf delta
echo :quit | spark-shell --packages io.delta:delta-core_2.12:1.0.0 --conf spark.databricks.delta.schema.autoMerge.enabled=true -i script-with-schema-evolution.scala