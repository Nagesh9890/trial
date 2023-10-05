from pyspark.sql import functions as F

def compute_aggregates(df, category_col, amount_col):
    # Pivot on the category column to compute counts and sums for each category
    agg_df = df.groupBy("account_number").pivot(category_col).agg(
        F.count(amount_col).alias("count"),
        F.sum(amount_col).alias("sum")
    )
    
    # Rename columns and add value type column based on the sum
    for category in df.select(category_col).distinct().rdd.flatMap(lambda x: x).collect():
        agg_df = agg_df.withColumnRenamed(category + "_count", "count_" + category) \
                      .withColumnRenamed(category + "_sum", "sum_" + category) \
                      .withColumn("type_" + category, 
                                  F.when(F.col("sum_" + category) == 0, "Null")
                                  .when(F.col("payee_account_type") == "SAVINGS", 
                                        F.when(F.col("sum_" + category) < 10000, "Low Value")
                                        .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                        .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
                                        .otherwise("Very High Value"))
                                  .when(F.col("payee_account_type") == "CURRENT", 
                                        F.when(F.col("sum_" + category) < 10000, "Low Value")
                                        .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                        .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
                                        .otherwise("Low Value only"))
                                  .otherwise("Unknown Type"))
    
    return agg_df

# Compute aggregates for category_level1 and category_level2
agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")

agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")

# Join the two aggregated DataFrames
final_agg_df = agg_df1.join(agg_df2, ["account_number"], "outer").fillna(0)

# Calculate the total transaction sum for each account
total_sum_df = result_df.groupBy("account_number").agg(F.sum("payer_amount").alias("total_transaction_sum"))

# Join with unique account details
unique_account_details = result_df.select("account_number", "account_holder_name", "account_ifsc", "account_type","payee_account_type","payer_account_type").distinct()
final_df = unique_account_details.join(final_agg_df, ["account_number"], "left").join(total_sum_df, ["account_number"], "left")

# Display the result
final_df.show()
------------------------


AnalysisExceptionTraceback (most recent call last)
<ipython-input-28-a2947757ac4f> in <module>()
     27 
     28 # Compute aggregates for category_level1 and category_level2
---> 29 agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")
     30 agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")
     31 

<ipython-input-28-a2947757ac4f> in compute_aggregates(df, category_col, amount_col)
     22                                         .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
     23                                         .otherwise("Low Value only"))
---> 24                                   .otherwise("Unknown Type"))
     25 
     26     return agg_df

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/sql/dataframe.pyc in withColumn(self, colName, col)
   1996         """
   1997         assert isinstance(col, Column), "col should be Column"
-> 1998         return DataFrame(self._jdf.withColumn(colName, col._jc), self.sql_ctx)
   1999 
   2000     @ignore_unicode_prefix

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py in __call__(self, *args)
   1255         answer = self.gateway_client.send_command(command)
   1256         return_value = get_return_value(
-> 1257             answer, self.gateway_client, self.target_id, self.name)
   1258 
   1259         for temp_arg in temp_args:

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/sql/utils.pyc in deco(*a, **kw)
     67                                              e.java_exception.getStackTrace()))
     68             if s.startswith('org.apache.spark.sql.AnalysisException: '):
---> 69                 raise AnalysisException(s.split(': ', 1)[1], stackTrace)
     70             if s.startswith('org.apache.spark.sql.catalyst.analysis'):
     71                 raise AnalysisException(s.split(': ', 1)[1], stackTrace)

AnalysisException: u"cannot resolve '`payee_account_type`' given input columns: [Personal Transfer_count, sum_Shopping, Personal Transfer_sum, count_Shopping, account_number];;\n'Project [account_number#945L, Personal Transfer_count#4459L, Personal Transfer_sum#4460L, count_Shopping#4470L, sum_Shopping#4476L, CASE WHEN (sum_Shopping#4476L = cast(0 as bigint)) THEN Null WHEN ('payee_account_type = SAVINGS) THEN CASE WHEN (sum_Shopping#4476L < cast(10000 as bigint)) THEN Low Value WHEN ((sum_Shopping#4476L >= cast(10000 as bigint)) && (sum_Shopping#4476L < cast(25000 as bigint))) THEN Medium Value WHEN ((sum_Shopping#4476L >= cast(25000 as bigint)) && (sum_Shopping#4476L < cast(50000 as bigint))) THEN High Value ELSE Very High Value END WHEN ('payee_account_type = CURRENT) THEN CASE WHEN (sum_Shopping#4476L < cast(10000 as bigint)) THEN Low Value WHEN ((sum_Shopping#4476L >= cast(10000 as bigint)) && (sum_Shopping#4476L < cast(25000 as bigint))) THEN Medium Value WHEN ((sum_Shopping#4476L >= cast(25000 as bigint)) && (sum_Shopping#4476L < cast(50000 as bigint))) THEN High Value ELSE Low Value only END ELSE Unknown Type END AS type_Shopping#4482]\n+- Project [account_number#945L, Personal Transfer_count#4459L, Personal Transfer_sum#4460L, count_Shopping#4470L, Shopping_sum#4462L AS sum_Shopping#4476L]\n   +- Project [account_number#945L, Personal Transfer_count#4459L, Personal Transfer_sum#4460L, Shopping_count#4461L AS count_Shopping#4470L, Shopping_sum#4462L]\n      +- Project [account_number#945L, __pivot_count(`payer_amount`) AS `count` AS `count(``payer_amount``) AS ``count```#4452[0] AS Personal Transfer_count#4459L, __pivot_sum(`payer_amount`) AS `sum` AS `sum(``payer_amount``) AS ``sum```#4458[0] AS Personal Transfer_sum#4460L, __pivot_count(`payer_amount`) AS `count` AS `count(``payer_amount``) AS ``count```#4452[1] AS Shopping_count#4461L, __pivot_sum(`payer_amount`) AS `sum` AS `sum(``payer_amount``) AS ``sum```#4458[1] AS Shopping_sum#4462L]\n         +- Aggregate [account_number#945L], [account_number#945L, pivotfirst(category_level1#1236, count(`payer_amount`) AS `count`#4445L, Personal Transfer, Shopping, 0, 0) AS __pivot_count(`payer_amount`) AS `count` AS `count(``payer_amount``) AS ``count```#4452, pivotfirst(category_level1#1236, sum(`payer_amount`) AS `sum`#4446L, Personal Transfer, Shopping, 0, 0) AS __pivot_sum(`payer_amount`) AS `sum` AS `sum(``payer_amount``) AS ``sum```#4458]\n            +- Aggregate [account_number#945L, category_level1#1236], [account_number#945L, category_level1#1236, count(payer_amount#960L) AS count(`payer_amount`) AS `count`#4445L, sum(payer_amount#960L) AS sum(`payer_amount`) AS `sum`#4446L]\n               +- Project [account_holder_name#943, account_ifsc#944, account_number#945L, account_type#946, created#947, customer_reference_number#948L, data_dt#949, mbanking_enabled#950L, note#951, payee_account_number#952, payee_account_type#953, payee_amount#954L, payee_ifsc#955, payee_name#956, payee_vpa#957, payer_account_number#958L, payer_account_type#959, payer_amount#960L, payer_ifsc#961, payer_name#962, payer_vpa#963, phone_number#964L, registered_at_acct#965, state#966, ... 6 more fields]\n                  +- Project [account_holder_name#943, account_ifsc#944, account_number#945L, account_type#946, created#947, customer_reference_number#948L, data_dt#949, mbanking_enabled#950L, note#951, payee_account_number#952, payee_account_type#953, payee_amount#954L, payee_ifsc#955, payee_name#956, payee_vpa#957, payer_account_number#958L, payer_account_type#959, payer_amount#960L, payer_ifsc#961, payer_name#962, payer_vpa#963, phone_number#964L, registered_at_acct#965, state#966, ... 7 more fields]\n                     +- Project [account_holder_name#943, account_ifsc#944, account_number#945L, account_type#946, created#947, customer_reference_number#948L, data_dt#949, mbanking_enabled#950L, note#951, payee_account_number#952, payee_account_type#953, payee_amount#954L, payee_ifsc#955, payee_name#956, payee_vpa#957, payer_account_number#958L, payer_account_type#959, payer_amount#960L, payer_ifsc#961, payer_name#962, payer_vpa#963, phone_number#964L, registered_at_acct#965, state#966, ... 6 more fields]\n                        +- Project [account_holder_name#943, account_ifsc#944, account_number#945L, account_type#946, created#947, customer_reference_number#948L, data_dt#949, mbanking_enabled#950L, note#951, payee_account_number#952, payee_account_type#953, payee_amount#954L, payee_ifsc#955, payee_name#956, payee_vpa#957, payer_account_number#958L, payer_account_type#959, payer_amount#960L, payer_ifsc#961, payer_name#962, payer_vpa#963, phone_number#964L, registered_at_acct#965, state#966, ... 5 more fields]\n                           +- Project [account_holder_name#943, account_ifsc#944, account_number#945L, account_type#946, created#947, customer_reference_number#948L, data_dt#949, mbanking_enabled#950L, note#951, payee_account_number#952, payee_account_type#953, payee_amount#954L, payee_ifsc#955, payee_name#956, payee_vpa#957, payer_account_number#958L, payer_account_type#959, payer_amount#960L, payer_ifsc#961, payer_name#962, payer_vpa#963, phone_number#964L, registered_at_acct#965, state#966, ... 4 more fields]\n                              +- LogicalRDD [Category1#941, Category2#942, account_holder_name#943, account_ifsc#944, account_number#945L, account_type#946, created#947, customer_reference_number#948L, data_dt#949, mbanking_enabled#950L, note#951, payee_account_number#952, payee_account_type#953, payee_amount#954L, payee_ifsc#955, payee_name#956, payee_vpa#957, payer_account_number#958L, payer_account_type#959, payer_amount#960L, payer_ifsc#961, payer_name#962, payer_vpa#963, phone_number#964L, ... 6 more fields], false\n"
