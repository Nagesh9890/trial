# trial

SELECT * FROM db_gold.gld_phone_pe_transactions LIMIT 100; 

----------------

SELECT upi_transaction_id, COUNT(*) as count
FROM db_gold.gld_phone_pe_transactions
WHERE upi_transaction_id = 'CITIBA3C555B801F41F8E0539082BBA9AF8'
GROUP BY upi_transaction_id
HAVING count > 1;
---------------
SELECT DISTINCT transaction_type, state 
FROM db_gold.gld_phone_pe_transactions
WHERE data_dt BETWEEN '2021-02-15' AND '2021-02-28';
-----------------------
SELECT DISTINCT transaction_type, state 
FROM db_gold.gld_phone_pe_transactions
WHERE data_dt = '2021-02-15';
------------------------------
SELECT DISTINCT transaction_type, state 
FROM db_gold.gld_phone_pe_transactions;

----------------------------
SELECT DISTINCT transaction_type 
FROM db_gold.gld_phone_pe_transactions;
-------------------

SELECT upi_transaction_id, COUNT(*) as count
FROM db_gold.gld_phone_pe_transactions
WHERE upi_transaction_id = 'CITIBA3C555B801F41F8E0539082BBA9AF8'
AND data_dt BETWEEN '2021-02-15' AND '2021-02-28'
GROUP BY upi_transaction_id
HAVING COUNT(*) > 1;


AnalysisException: Could not resolve column/field reference: 'count'




















































from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("View Top 100 Records") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the table from Hive
df = spark.read.table("db_gold.gld_phone_pe_transactions")

# Display the top 100 records
df.limit(100).show()















----------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import last_day, month, year, col, current_date, datediff
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

-----------------------

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

# Convert the specified date to a date object and collect its value
specified_date_value = df.select(to_date(lit(specified_date))).collect()[0][0]

# Collect the result of last_day function
last_date_of_month = df.select(last_day(to_date(lit(specified_date)))).collect()[0][0]

# Check if the specified date is the last day of the month
if specified_date_value == last_date_of_month:
    # Filter the data to get the last one month's data from the specified date
    df_filtered = df.filter(datediff(to_date(lit(specified_date)), col("data_date")) <= 30)
    df_filtered.show()
else:
    print "{} is not the last day of the month. Job will not proceed.".format(specified_date)
