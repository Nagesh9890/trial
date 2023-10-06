from pyspark.sql import functions as F

def compute_aggregates(df, category_col, amount_col):
    # Pivot on the category column to compute counts and sums for each category
    agg_df = df.groupBy("payer_account_number", "payer_account_type").pivot(category_col).agg(
        F.count(amount_col).alias("count"),
        F.sum(amount_col).alias("sum")
    )
    
    # Rename columns and add value type column based on the sum
    for category in df.select(category_col).distinct().rdd.flatMap(lambda x: x).collect():
        agg_df = agg_df.withColumnRenamed(category + "_count", "count_" + category) \
                      .withColumnRenamed(category + "_sum", "sum_" + category) \
                      .withColumn("type_" + category, 
                                  F.when((F.col("count_" + category) == 0) | (F.col("sum_" + category).isNull()) | (F.col("sum_" + category) == 0), "No transactions")
                                  .when(F.col("payer_account_type") == "SAVINGS", 
                                        F.when(F.col("sum_" + category) < 5000000, "10")
                                        .when((F.col("sum_" + category) >= 5000000) & (F.col("sum_" + category) < 10000000), "9")
                                        .when((F.col("sum_" + category) >= 10000000) & (F.col("sum_" + category) < 15000000), "8")
                                        .when((F.col("sum_" + category) >= 15000000) & (F.col("sum_" + category) < 20000000), "7")
                                        .when((F.col("sum_" + category) >= 20000000) & (F.col("sum_" + category) < 25000000), "6")
                                        .when((F.col("sum_" + category) >= 25000000) & (F.col("sum_" + category) < 30000000), "5")
                                        .when((F.col("sum_" + category) >= 30000000) & (F.col("sum_" + category) < 35000000), "4")
                                        .when((F.col("sum_" + category) >= 35000000) & (F.col("sum_" + category) < 40000000), "3")
                                        .when((F.col("sum_" + category) >= 40000000) & (F.col("sum_" + category) < 45000000), "2")
                                        .otherwise("1"))
                                  .when(F.col("payer_account_type") == "CURRENT", 
                                        F.when(F.col("sum_" + category) < 15000000, "10")
                                        .when((F.col("sum_" + category) >= 15000000) & (F.col("sum_" + category) < 30000000), "9")
                                        .when((F.col("sum_" + category) >= 30000000) & (F.col("sum_" + category) < 45000000), "8")
                                        .when((F.col("sum_" + category) >= 45000000) & (F.col("sum_" + category) < 60000000), "7")
                                        .when((F.col("sum_" + category) >= 60000000) & (F.col("sum_" + category) < 75000000), "6")
                                        .when((F.col("sum_" + category) >= 75000000) & (F.col("sum_" + category) < 90000000), "5")
                                        .when((F.col("sum_" + category) >= 90000000) & (F.col("sum_" + category) < 105000000), "4")
                                        .when((F.col("sum_" + category) >= 105000000) & (F.col("sum_" + category) < 120000000), "3")
                                        .when((F.col("sum_" + category) >= 120000000) & (F.col("sum_" + category) < 135000000), "2")
                                        .otherwise("1"))
                                  .otherwise("Unknown Type"))
    
    return agg_df

# Compute aggregates for category_level1 and category_level2 
agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")
agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")

# Join the two aggregated DataFrames 
final_agg_df = agg_df1.join(agg_df2, ["payer_account_number", "payer_account_type"], "outer").fillna(0) 

# Calculate the total transaction sum for each account
total_sum_df = result_df.groupBy("payer_account_number").agg(F.sum("payer_amount").alias("total_transaction_sum_paid_as_payer"))

# Calculate how many times each payer_account_number appears in payee_account_number and the sum of payee_amount
appearance_df = result_df.groupBy("payee_account_number") \
                         .agg(F.count("*").alias("as_a_payee_count"), 
                              F.sum("payee_amount").alias("as_a_payee_sum_recieved"))

# Join with unique account details 
unique_account_details = result_df.select("payer_account_number", "account_holder_name", "account_ifsc", "payer_account_type","payee_account_type","payer_account_type").distinct()
final_df = unique_account_details.join(final_agg_df, ["payer_account_number", "payer_account_type"], "left") \
                                 .join(total_sum_df, ["payer_account_number"], "left") \
                                 .join(appearance_df, unique_account_details.payer_account_number == appearance_df.payee_account_number, "left") \
                                 .drop("payee_account_number") \
                                 .fillna(0)

# Display the result
final_df.show()
