# trial

SELECT * FROM db_gold.gld_phone_pe_transactions LIMIT 100; 

----------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import last_day, month, year, col, current_date, datediff

# Create a Spark session
spark = SparkSession.builder \
    .appName("Filter Last Date of Month") \
    .getOrCreate()

# Sample data
data = [("2023-08-01",), ("2023-08-15",), ("2023-08-31",), ("2023-09-01",), ("2023-09-10",), ("2023-09-30",)]
df = spark.createDataFrame(data, ["date_column"])

# Check if today's date is the last day of the month
today = current_date()
if today == last_day(today):
    # Filter the data to get the last one month's data from the current date
    df_filtered = df.filter(datediff(today, col("date_column")) <= 30)
    df_filtered.show()
else:
    print("Today is not the last day of the month. Job will not proceed.")

