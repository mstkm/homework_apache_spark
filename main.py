import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master("local")
        .appName("Homework Apache Spark")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
)

"""
Read Data
"""
file_path = os.getcwd()
csv_file_name = 'laptop_pricing_dataset.csv'
df = spark.read.csv(file_path+'/data/'+csv_file_name, header=True, inferSchema=True)
df.show()

"""
3a. How the average RAM GB per manufacturer ? which manufacturer has higher average RAM ?
"""
df.createOrReplaceTempView("laptop_pricing")
df1 = spark.sql("SELECT Manufacturer, ROUND(AVG(RAM_GB), 2) AS Average_RAM_GB FROM laptop_pricing GROUP BY Manufacturer ORDER BY Average_RAM_GB DESC")
df2 = df1.collect()
df1.show()
print(df2[0].Manufacturer+' has the highest average GB RAM.')

"""
3b. Calculate the average screen size per Screen Type, and order it by average from higher to lowest.
"""
df.createOrReplaceTempView("laptop_pricing")
spark.sql("SELECT Screen, ROUND(AVG(Screen_Size_cm), 2) AS Average_Screen_Size_cm FROM laptop_pricing GROUP BY Screen ORDER BY Average_Screen_Size_cm DESC").show()

"""
3c. Profiling by manufacturer and screen type, which manufacturer has more IPS Panel product, Full HD product, and so on by counting the record by those two columns.
"""
df.createOrReplaceTempView("laptop_pricing")
spark.sql("SELECT Manufacturer, Screen, COUNT(Screen) Total_Unit FROM laptop_pricing GROUP BY Manufacturer, Screen ORDER BY Manufacturer").show()