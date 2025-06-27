
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts","true")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config("spark.driver.allowMultipleContexts","true").getOrCreate()
print("DONE==== START YOUR WORK👇 ")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Practice").getOrCreate()

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

columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.show()


seldf = df.select("id", "name", "department", "salary")
seldf.show()

fildf = df.filter("department ='Sales'")
fildf.show()

sindf = df.select("name")
sindf.show()

muldf = df.select("name", "salary")
muldf.show()

mulfil = df.filter("department = 'HR' and salary > 45000")
mulfil.show()

alpdf = df.filter("name like 'A%'")
alpdf.show()

negdf = df.filter ("department != 'Finance'")
negdf.show()

logdf = df.filter("salary > 40000 and department = 'Sales'")
logdf.show()

aldf = df.select(df.name.alias("employee_name"), df.salary.alias("emp_salary") )
aldf.show()

casedf = df.filter("name = 'Alice'")
casedf.show()

