# trial

SELECT * FROM db_gold.gld_phone_pe_transactions LIMIT 100; 

----------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import last_day, month, year, col, current_date, datediff



​from pyspark.sql import SparkSession
from pyspark.sql.functions import last_day, datediff, current_date, col

# Create a Spark session
spark = SparkSession.builder \
    .appName("Filter Last Date of Month") \
    .getOrCreate()

# Sample data
data = [("2023-08-01",), ("2023-08-15",), ("2023-08-31",), ("2023-09-01",), ("2023-09-10",), ("2023-09-30",)]
df = spark.createDataFrame(data, ["date_column"])

# Retrieve the value of today and the last day of the month
today_value = df.select(current_date()).collect()[0][0]
last_day_value = df.select(last_day(current_date())).collect()[0][0]

# Check if today's date is the last day of the month
if today_value == last_day_value:
    # Filter the data to get the last one month's data from the current date
    df_filtered = df.filter(datediff(current_date(), col("date_column")) <= 30)
    df_filtered.show()
else:
    print "Today is not the last day of the month. Job will not proceed."

----------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import last_day, datediff, lit, col, to_date

# Create a Spark session
spark = SparkSession.builder \
    .appName("Filter Last Date of Month") \
    .getOrCreate()

# Sample data
data = [("2023-08-01",), ("2023-08-15",), ("2023-08-31",), ("2023-09-01",), ("2023-09-10",), ("2023-09-30",)]
df = spark.createDataFrame(data, ["data_date"])

# Specify any date you want to check against
specified_date = "2023-09-30"  # Example date, you can change this

# Convert the specified date to a date object
specified_date_obj = to_date(lit(specified_date))

# Collect the result of last_day function
last_date_of_month = df.select(last_day(specified_date_obj)).collect()[0][0]

# Check if the specified date is the last day of the month
if specified_date_obj == last_date_of_month:
    # Filter the data to get the last one month's data from the specified date
    df_filtered = df.filter(datediff(specified_date_obj, col("data_date")) <= 30)
    df_filtered.show()
else:
    print "{} is not the last day of the month. Job will not proceed.".format(specified_date)

-----------------------

ValueErrorTraceback (most recent call last)
<ipython-input-16-f1c1d7d00f0c> in <module>()
     19 
     20 # Check if the specified date is the last day of the month
---> 21 if specified_date_obj == last_date_of_month:
     22     # Filter the data to get the last one month's data from the specified date
     23     df_filtered = df.filter(datediff(specified_date_obj, col("data_date")) <= 30)

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/sql/column.pyc in __nonzero__(self)
    688 
    689     def __nonzero__(self):
--> 690         raise ValueError("Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
    691                          "'~' for 'not' when building DataFrame boolean expressions.")
    692     __bool__ = __nonzero__

ValueError: Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
