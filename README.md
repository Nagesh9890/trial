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

# ... (rest of your code remains the same)
