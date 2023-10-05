from pyspark.sql import functions as F
import pandas as pd

# ... (rest of your code remains the same)

# Convert the PySpark DataFrame to a Pandas DataFrame
pandas_df = final_df.toPandas()

# Write the Pandas DataFrame to an Excel file
with pd.ExcelWriter('output.xlsx') as writer:
    pandas_df.to_excel(writer, index=False)

    
