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
                                        F.when(F.col("sum_" + category) < 10000, "Low Value")
                                        .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                        .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
                                        .otherwise("Very High Value"))
                                  .when(F.col("payer_account_type") == "CURRENT", 
                                        F.when(F.col("sum_" + category) < 10000, "Low Value")
                                        .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                        .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000000), "High Value")
                                        .otherwise("Low Value only"))
                                  .otherwise("Unknown Type"))
    
    return agg_df

# Compute aggregates for category_level1 and category_level2 
agg_df1 = compute_aggregates(result_df, "category_level1", "payer_amount")
agg_df2 = compute_aggregates(result_df, "category_level2", "payer_amount")

# Join the two aggregated DataFrames 
final_agg_df = agg_df1.join(agg_df2, ["payer_account_number", "payer_account_type"], "outer").fillna(0) 

# Calculate the total transaction sum for each account
total_sum_df = result_df.groupBy("payer_account_number").agg(F.sum("payer_amount").alias("total_transaction_sum"))

# Calculate how many times each payer_account_number appears in payee_account_number and the sum of payee_amount
appearance_df = result_df.groupBy("payee_account_number") \
                         .agg(F.count("*").alias("appearance_count"), 
                              F.sum("payee_amount").alias("appearance_sum"))

# Join with unique account details 
unique_account_details = result_df.select("payer_account_number", "account_holder_name", "account_ifsc", "payer_account_type","payee_account_type","payer_account_type").distinct()
final_df = unique_account_details.join(final_agg_df, ["payer_account_number", "payer_account_type"], "left") \
                                 .join(total_sum_df, ["payer_account_number"], "left") \
                                 .join(appearance_df, unique_account_details.payer_account_number == appearance_df.payee_account_number, "left") \
                                 .drop("payee_account_number") \
                                 .fillna(0)

# Display the result
final_df.show()

