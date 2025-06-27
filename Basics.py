from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Practice") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.allowMultipleContexts", "true") \
    .getOrCreate()

print("DONE==== START YOUR WORK 👇")

# Sample data
data = [
    (1, "Alice", "Sales", 30000),
    (2, "Bob", "HR", 40000),
    (3, "Charlie", "Sales", 50000),
    (4, "David", "Finance", 60000),
    (5, "Eve", "HR", 55000),
    (6, "Frank", "Admin", 45000),
    (7, "Grace", "Finance", 52000),
    (8, "Arun", "HR", 30000)
]

# Column names
columns = ["id", "name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()


# Select all columns
seldf = df.select("id", "name", "department", "salary")
seldf.show()

# Filter by department using col()
fildf = df.filter(col("department") == "Sales")
fildf.show()

# Select single column
sindf = df.select("name")
sindf.show()

# Select multiple columns
muldf = df.select("name", "salary")
muldf.show()

# Multiple filters with col()
mulfil = df.filter((col("department") == "HR") & (col("salary") > 45000))
mulfil.show()

# Name starts with A
alpdf = df.filter(col("name").like("A%"))
alpdf.show()

# Negation filter
negdf = df.filter(col("department") != "Finance")
negdf.show()

# Logical AND
logdf = df.filter((col("salary") > 40000) & (col("department") == "Sales"))
logdf.show()

# Aliased columns
aldf = df.select(col("name").alias("employee_name"), col("salary").alias("emp_salary"))
aldf.show()

# Case-sensitive filter
casedf = df.filter(col("name") == "Alice")
casedf.show()

