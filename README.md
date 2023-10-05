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
                      .withColumnRenamed(category + "_sum", "sum_" + category)
        
        # Apply logic based on payee_account_type
        if df.filter(F.col("payee_account_type") == "SAVINGS").count() > 0:
            agg_df = agg_df.withColumn("type_" + category, 
                                      F.when(F.col("sum_" + category) < 10000, "Low Value")
                                      .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                      .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
                                      .otherwise("Very High Value"))
        elif df.filter(F.col("payee_account_type") == "CURRENT").count() > 0:
            agg_df = agg_df.withColumn("type_" + category, 
                                      F.when(F.col("sum_" + category) < 10000, "Low Value")
                                      .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                      .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
                                      .otherwise("Low Value only"))
    
    return agg_df

# Filter the DataFrame based on payee_account_type
filtered_df = result_df.filter(F.col("payee_account_type").isin(["SAVINGS", "CURRENT"]))

# Compute aggregates for category_level1 and category_level2
agg_df1 = compute_aggregates(filtered_df, "category_level1", "payer_amount")
agg_df2 = compute_aggregates(filtered_df, "category_level2", "payer_amount")

# Join the two aggregated DataFrames
final_agg_df = agg_df1.join(agg_df2, ["account_number"], "outer").fillna(0)

# Calculate the total transaction sum for each account
total_sum_df = filtered_df.groupBy("account_number").agg(F.sum("payer_amount").alias("total_transaction_sum"))

# Join with unique account details
unique_account_details = filtered_df.select("account_number", "account_holder_name", "account_ifsc", "account_type","payee_account_type","payer_account_type").distinct()
final_df = unique_account_details.join(final_agg_df, ["account_number"], "left").join(total_sum_df, ["account_number"], "left")

# Display the result
final_df.show()
