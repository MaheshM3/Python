spark.conf.set("spark.sql.shuffle.partitions", <optimal_value>)  # Start with 2-3 times the cluster cores
spark.conf.set("spark.default.parallelism", <optimal_value>)
spark.conf.set("spark.executor.memory", "10g")  # Adjust as needed
spark.conf.set("spark.executor.memoryOverhead", "2g")  # At least 10-20% of executor memory
spark.conf.set("spark.memory.fraction", 0.7)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "134217728")  # 128 MB
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.shuffle.file.buffer", "64k")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50 MB
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")
spark.conf.set("spark.shuffle.io.maxRetries", "10")
spark.conf.set("spark.shuffle.io.retryWait", "5s")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")


# Increase number of partitions for better balance
rdd = rdd.repartition(64)  # Or set based on your cluster size

# Memory tuning for cached RDDs
spark.conf.set("spark.memory.storageFraction", 0.6)

# Enable AQE for dynamic shuffle partitioning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "134217728")  # 128 MB
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Set locality wait to improve data locality
spark.conf.set("spark.locality.wait", "1s")
