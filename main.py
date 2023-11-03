# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F

# Initialize a Spark session
spark = SparkSession.builder.appName("Week10DataProcessing").getOrCreate()

# Load your large dataset (replace 'your_dataset_path' with the actual path)
data = spark.read.csv("movies.csv", header=True, inferSchema=True)

# Data Transformation Example:
# You can perform various data transformations like filtering, grouping, or adding new columns
transformed_data = data.select("Film", "Genre", "Worldwide Gross").filter(col("Audience score %") > 50)

# Spark SQL Query Example:
# You can use Spark SQL to query the data
data.createOrReplaceTempView("data_table")
sql_result = spark.sql("SELECT Year, AVG(Profitability) AS AvgProfitability FROM data_table GROUP BY Year")

# Save the transformed data if needed
transformed_data.write.csv("transformed", header=True, mode="overwrite")

# save the sql_result 
sql_result.write.csv("sql_result", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
