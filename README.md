# with Segmentation based of Sum of categories transfer 

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
                                  F.when(F.col("sum_" + category) > 30000, "High Value")
                                  .otherwise("Low Value"))
    
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
----------------------

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
                                  F.when(F.col("sum_" + category) < 10000, "Low Value")
                                  .when((F.col("sum_" + category) >= 10000) & (F.col("sum_" + category) < 25000), "Medium Value")
                                  .when((F.col("sum_" + category) >= 25000) & (F.col("sum_" + category) < 50000), "High Value")
                                  .otherwise("Very High Value"))
    
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

