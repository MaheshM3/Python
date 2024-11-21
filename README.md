# Help
## Step 4: Install Required Build Tools
	Make sure you have setuptools and wheel installed:
		pip install setuptools wheel build

## Step 5: Build the Wheel
	To create the wheel, navigate to the root directory (where pyproject.toml is located) and run:
		python -m build

## Step 6: Check the Output
	After running the build command, a dist directory will be created, and you should see your .whl file inside:
		dist/
		└── your_package_name-0.1.0-py3-none-any.whl

## Step 7: Install the Wheel Locally (Optional)
	You can test the wheel installation using pip:
	pip install dist/your_package_name-0.1.0-py3-none-any.whl


## Recommended Spark Configuration for Standard_L32s_v3

### Enable Delta Cache for local caching of frequently accessed Delta Lake data
	spark.databricks.io.cache.enabled true

### Enable Auto Optimize and Auto Compaction to manage Delta Lake files and reduce I/O
	spark.databricks.delta.optimizeWrite.enabled true
	spark.databricks.delta.autoCompact.enabled true

### Enable Adaptive Query Execution (AQE) for dynamic optimization of partitions and shuffles
	spark.sql.adaptive.enabled true
	spark.sql.adaptive.coalescePartitions.enabled true
	spark.sql.adaptive.advisoryPartitionSizeInBytes 64MB

### Set shuffle partitions based on expected data size (adjust as needed)
	spark.sql.shuffle.partitions 200

### Increase network timeout for large data retrievals
	spark.network.timeout 800s

### Set memory and transaction log settings for Delta Lake tables
	spark.driver.memory 32g
	spark.databricks.delta.retentionDurationCheck.enabled false
	spark.databricks.delta.logRetentionDuration 30 days

### Enable dynamic partition pruning to reduce data size during filtered reads
	spark.sql.optimizer.dynamicPartitionPruning.enabled true

### Adjust Arrow batch size if using `toPandas()` conversions for large datasets
	spark.sql.execution.arrow.maxRecordsPerBatch 5000

### Adjust Arrow batch size for better memory efficiency during conversion to Pandas
	spark.sql.execution.arrow.maxRecordsPerBatch 5000

## GIT
	Ensure you are on the feature/dev branch locally:
	git fetch origin                    # Fetch the latest changes from the remote
	git checkout -b feature/dev-merged  # Create a new branch
	git merge origin/feature/dev        # Merge the latest remote changes into the new branch
	# Resolve conflicts if any
	git add .                           # Stage resolved files
	git commit -m "Resolve conflicts and merge changes"  # Commit the merge
	git push origin feature/dev-merged  # Push the new branch to the remote

