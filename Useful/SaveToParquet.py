from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    ArrayType
)
from project_function import decyzja_kredytowa

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # json message schema
    json_schema = StructType(
        [
            StructField("ID", StringType()),
            StructField("Outstanding_Debt", FloatType()),
            StructField("Credit_Mix", StringType()),
            StructField("Num_Credit_Card", IntegerType()),
            StructField("Interest_Rate", IntegerType()),
        ]
    )
    
    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .load()
    )
    
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("ID").alias("ID"),
        f.col("json").getField("Outstanding_Debt").alias("Outstanding_Debt"),
        f.col("json").getField("Credit_Mix").alias("Credit_Mix"),
        f.col("json").getField("Num_Credit_Card").alias("Num_Credit_Card"),
        f.col("json").getField("Interest_Rate").alias("Interest_Rate"),
    )
    
    decyzja_kredytowa_udf = f.udf(decyzja_kredytowa, ArrayType(StringType()))
    
    parsed = parsed.withColumn("Decision",decyzja_kredytowa_udf(
        f.col("Outstanding_Debt"), 
        f.col("Credit_Mix"),
        f.col("Num_Credit_Card"), 
        f.col("Interest_Rate")
    )
                              )

    # defining output
    def foreach_batch_function(df, epoch_id):
        df.write.format("parquet").mode("append").save("df")

    query = parsed.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
    query.awaitTermination()
