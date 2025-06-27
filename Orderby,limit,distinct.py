
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts","true")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config("spark.driver.allowMultipleContexts","true").getOrCreate()
print("DONE==== START YOUR WORK👇 ")

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("InterviewData").getOrCreate()

# Employees DataFrame
employees_data = [
    (1, 'Alice', 'Sales', 60000),
    (2, 'Bob', 'HR', 50000),
    (3, 'Charlie', 'Sales', 75000),
    (4, 'David', 'Engineering', 80000),
    (5, 'Eva', 'Engineering', 70000),
    (6, 'Frank', 'HR', 48000),
    (7, 'Grace', 'Marketing', 62000),
    (8, 'Heidi', 'Marketing', 58000)
]
employees_df = spark.createDataFrame(employees_data, ["emp_id", "emp_name", "department", "salary"])

# Customers DataFrame
customers_data = [
    (1, 'John', 'Delhi'),
    (2, 'Jane', 'Mumbai'),
    (3, 'Alice', 'Delhi'),
    (4, 'Bob', 'Bangalore'),
    (5, 'Alice', 'Chennai'),
    (6, 'Tom', 'Mumbai')
]
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "city"])

# Sales DataFrame
sales_data = [
    (101, 'Laptop', 15, '2024-12-01'),
    (102, 'Tablet', 25, '2024-12-05'),
    (103, 'Phone', 40, '2024-12-06'),
    (104, 'Monitor', 10, '2024-12-03'),
    (105, 'Keyboard', 5, '2024-12-07')
]
sales_df = spark.createDataFrame(sales_data, ["product_id", "product_name", "quantity_sold", "sale_date"])


#Product DataFrame
products_data = [
    (201, 'Laptop', 85000),
    (202, 'Tablet', 30000),
    (203, 'Phone', 50000),
    (204, 'Monitor', 12000),
    (205, 'Keyboard', 2000),
    (206, 'Mouse', 1500),
    (207, 'Speaker', 4000),
    (208, 'Printer', 7000),
    (209, 'Webcam', 2500),
    (210, 'Desk Lamp', 1800)
]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "product_prices"])


#Organisation Dataframe

organization_data = [
    (1, 'Alice', 'Manager'),
    (2, 'Bob', 'Developer'),
    (3, 'Charlie', 'Analyst'),
    (4, 'David', 'Developer'),
    (5, 'Eva', 'HR'),
    (6, 'Frank', 'Manager'),
    (7, 'Grace', 'Designer'),
    (8, 'Heidi', 'Analyst')
]

organization_df = spark.createDataFrame(organization_data, ["emp_id", "emp_name", "jobs"])

#Order Dataframe

orders_data = [
    (1001, 1, '2024-12-01'),
    (1002, 3, '2024-12-03'),
    (1003, 2, '2024-12-05'),
    (1004, 5, '2024-12-06'),
    (1005, 4, '2024-12-07'),
    (1006, 6, '2024-12-08')
]

orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_date"])

#user Dataframe

users_data = [
    (1, 'John', 'Doe'),
    (2, 'Jane', 'Smith'),
    (3, 'Alice', 'Brown'),
    (4, 'Bob', 'Johnson'),
    (5, 'Alice', 'Brown'),  # duplicate
    (6, 'Tom', 'Hardy')
]

users_df = spark.createDataFrame(users_data, ["user_id", "first_name", "last_name"])


from pyspark.sql.functions import col

# Top 5 highest paid employees
emp = employees_df.orderBy(col("salary").desc()).limit(5)
emp.show()

# All customers ordered alphabetically by name (ascending)
cus = customers_df.orderBy(col("name").asc())
cus.show()

# Top 3 products with highest quantity sold
sal = sales_df.orderBy(col("quantity_sold").desc()).limit(3)
sal.show()

# Top 10 cheapest products based on price
pro = products_df.orderBy(col("product_prices").asc()).limit(10)
pro.show()

# Display all unique departments from the employees table
empdis = employees_df.select("department").distinct()
empdis.show()

# Count of unique cities in the customers table
ccus = customers_df.select("city").distinct().count()
print("Number of distinct cities:", ccus)

# First 5 orders sorted by order date (oldest first)
ord = orders_df.orderBy(col("order_date").asc()).limit(5)
ord.show()

# Display all unique job roles in the organization
job = organization_df.select(col("jobs")).distinct()
job.show()

# Display all unique combinations of first and last names from users
com = user


