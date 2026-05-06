import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, isnan, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("StockETL").getOrCreate()

# Transform and clean stock data using Pandas
def transform_stock_data(df, symbol):
    print(f'Transforming data for {symbol}...')

    # Fix multi-index columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]
    df.columns = [str(col).lower().replace(' ', '_') for col in df.columns]
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['symbol'] = symbol

    # Clean the data
    df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
    df = df.dropna()

    # Add new columns
    df['daily_return'] = df['close'].pct_change()
    df['weekly_return'] = df['close'].pct_change(periods=5)
    df['monthly_return'] = df['close'].pct_change(periods=21)

    return df

# Scalable transformation using PySpark for larger datasets
def transform_with_pyspark(df_list):
    print('Performing scalable transformations with PySpark...')

    # Convert list of DataFrames to Spark DataFrame
    spark_df = spark.createDataFrame(pd.concat(df_list, ignore_index=True))

    # Deduplication
    window_spec = Window.partitionBy("symbol", "date").orderBy(col("date").desc())
    spark_df = spark_df.withColumn("row_num", row_number().over(window_spec))
    spark_df = spark_df.filter(col("row_num") == 1).drop("row_num")

    # Handle nulls
    spark_df = spark_df.na.drop()

    # Calculate returns
    window_spec_returns = Window.partitionBy("symbol").orderBy("date")
    spark_df = spark_df.withColumn("prev_close", lag("close").over(window_spec_returns))
    spark_df = spark_df.withColumn("daily_return", when(col("prev_close").isNotNull(),
                                                         (col("close") - col("prev_close")) / col("prev_close")).otherwise(None))

    # Aggregations (example: average volume per symbol)
    agg_df = spark_df.groupBy("symbol").agg({"volume": "avg", "close": "max"})

    # Join back (example join)
    spark_df = spark_df.join(agg_df, on="symbol", how="left")

    return spark_df.toPandas()
