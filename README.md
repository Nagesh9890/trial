# trial

 Confluent Cloud dashboard comprised several key sections. Here's a breakdown:

Clusters: This is where you can view and manage your Kafka clusters. You can create new clusters, delete existing ones, and configure them. This section provides details about:

Broker count and storage
Throughput and latency metrics
Cloud provider and region details
Topics: Topics are fundamental to Kafka. They are channels to which messages are sent by producers and from which messages are consumed by consumers. In this section, you can:

Create, view, and manage topics
Monitor topic-level metrics like throughput, number of messages, etc.
Set retention policies and configurations
Consumers: This section provides insights into the consumers that are reading messages from your topics. You can:

View active consumer groups
Monitor their lag (how far behind they are from the latest message)
Get details on the topics they are consuming from and their offsets
Producers: Like the consumers' section, this provides insights but for entities producing messages to your topics.

Connect: Confluent Cloud offers a fully managed Kafka Connect service. With this:

You can deploy connector instances
Monitor their health and throughput
Manage and configure connectors
KSQL: KSQL is a streaming SQL engine for Apache Kafka. With this section, you can:

Create and manage persistent queries
Monitor KSQL application performance
Write and test KSQL queries
Schema Registry: This is where you can manage Avro schemas used by your Kafka topics. Features include:

Viewing, adding, and editing schemas
Versioning of schemas
Compatibility checks
Billing and Usage: As a cloud service, Confluent Cloud charges users based on usage. This section provides:

Detailed breakdowns of costs
Usage metrics, such as data transfer, storage, and compute
Settings & Administration: This section lets you:

Manage user access and roles
Set up multi-factor authentication and other security features
Configure API keys and access controls
Support & Documentation: Here, you can access:

Confluent's knowledge base
Documentation
Support tickets and contact details
Marketplace: Confluent Cloud also features a marketplace where you can discover and deploy partner solutions, connectors, and more.



Stream lineage helps in several ways:

Understanding Data Flow: It provides a visual representation of how data is moving through the system. This is essential for architects, developers, and operations teams to understand the architecture and diagnose issues.

Impact Analysis: If there's a need to change or upgrade a part of the system, stream lineage can help determine what other components might be affected by that change.

Audit & Compliance: For industries where data governance is crucial, stream lineage can provide a trace of where data comes from and where it goes, which is essential for audits.

Error Tracking: If there's an anomaly or error in the data, stream lineage can help trace back to its source, aiding in faster resolution.

Optimization: By understanding the flow and transformation of data, teams can identify bottlenecks or inefficiencies and optimize them.

In the context of platforms like Kafka, stream lineage might involve:

Producers: Where is the data coming from?
Topics: Through which topics is the data passing?
Streams/KSQL/Table Operations: Are there any transformations, aggregations, or joins happening?
Connectors: Are there any connectors sourcing or sinking data to external systems?
Consumers: Who is consuming the data and for what purpose?
Visual tools, sometimes part of larger data governance platforms or offered by vendors like Confluent, provide stream lineage capabilities. These tools scan metadata, capture data flow, and then visually represent the lineage to users.


1. Producer:
A producer is an entity or application that sends (or produces) records to Kafka topics. In the broader Kafka ecosystem:

Role: Sends data to Kafka topics.
API: Uses the Kafka producer API to serialize and send records.
Data Source: Can be any application, service, or system that wants to send data to Kafka. It might be a web application, a logging system, a database, etc.
Configuration: Involves settings related to batching, retries, serialization (like Avro, JSON, String), acknowledgments, etc.
2. Sink (in Kafka Connect):
A sink is a type of Kafka Connect connector. While Kafka Connect itself is a framework to stream data in and out of Kafka, it distinguishes between two types of connectors: source connectors and sink connectors.

Role: A sink connector consumes records from Kafka topics and sends them to external systems or databases.
API: Uses the Kafka consumer API internally to fetch records from Kafka topics. The fetched data is then passed to the specific implementation of the sink connector to be sent to the target system.
Data Destination: Can be any external system like a relational database (e.g., PostgreSQL, MySQL), a search index (like Elasticsearch), cloud storage (like S3), or any other system supported by a sink connector implementation.
Configuration: Involves settings related to the target system (e.g., connection details, credentials), topics to consume from, deserialization methods, error handling, and sometimes transformations to apply to the data before sending it to the target system.
To summarize:

A producer is a general term in Kafka that refers to anything that sends data to Kafka topics.
A sink (in the context of Kafka Connect) specifically refers to a connector that takes data from Kafka topics and sends it to an external system.














































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


#optimized one
from pyspark.sql import functions as F

def compute_aggregates(df, category_col, amount_col):
    # Pivot on the category column to compute counts and sums for each category
    agg_df = df.groupBy("account_number").pivot(category_col).agg(
        F.count(amount_col).alias("count"),
        F.sum(amount_col).alias("sum")
    )
    
    # Rename columns to match the desired format
    for category in df.select(category_col).distinct().rdd.flatMap(lambda x: x).collect():
        agg_df = agg_df.withColumnRenamed(category + "_count", "count_" + category) \
                      .withColumnRenamed(category + "_sum", "sum_" + category)
    
    return agg_df

# Compute aggregates for category_level1 and category_level2
agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")
agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")

# Join the two aggregated DataFrames
final_agg_df = agg_df1.join(agg_df2, ["account_number"], "outer").fillna(0)

# Join with unique account details
unique_account_details = result_df.select("account_number", "account_holder_name", "account_ifsc", "account_type").distinct()
final_df = unique_account_details.join(final_agg_df, ["account_number"], "left")

# Display the result
final_df.show()
