from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, from_json, to_timestamp
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import timezone,datetime

def persistData(dataframe, batchId):

      # Write to Cassandra
      data = dataframe.rdd.collect()
      print("Writing data to InfluxDB")
      for i in data:
            crypto_name = i["crypto_name"]
            timeValue =  datetime.strptime(i["time"], "%Y-%m-%d %H:%M:%S.%f")
            crypto_eur = float(i["ada_eur"])
            crypto_usd = float(i["ada_usd"])
            eur_usd = float(i["eur_usd"])
            usd_eur = float(i["usd_eur"])
            
            crypto_eur_point = Point(crypto_name).field(crypto_name+"/EUR",
                                          crypto_eur).time(time=timeValue)
            crypto_usd_point = Point(crypto_name).field(crypto_name+"/USD",
                                          crypto_usd).time(time=timeValue)
            eur_usd_point = Point(crypto_name).field("EUR/USD",
                                          eur_usd).time(time=timeValue)      
            usd_eur_point = Point(crypto_name).field("USD/EUR",
                                          usd_eur).time(time=timeValue)

            influxClient.write_api(write_options=SYNCHRONOUS).write(
                  bucket=bucket, record=[crypto_eur_point, crypto_usd_point, eur_usd_point, usd_eur_point])
                  
      # Saving to Cassandra
      print(f"Writing DataFrame to Cassandra (micro-batch {batchId})")
      dataframe = dataframe.drop("time")
      (dataframe
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace="test", table="currencies")
        .mode("append")
        .save())
      


# InfluxDB configuration
influxClient = InfluxDBClient.from_config_file("influxdb.ini")
bucket = "100410977_Bucket"


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Kafka-DataFrame-StdOut")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .getOrCreate())

# 1. Input data: DataFrame from Apache Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "crypto_exchange")
      .load())


# 2. Data processing: read json as dataframe and calculate currencies
schema = StructType([
      StructField("time", StringType(), True),
      StructField("crypto_name", StringType(), True),
      StructField("ADA/EUR", FloatType(), True),
      StructField("ADA/USD", FloatType(), True),
])

values = df.selectExpr("CAST(value AS STRING)", "timestamp")
dfjson = values.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")
dfjson = dfjson.withColumn("timestamp", col("time"))
dfjson = dfjson.withColumn("eur_usd", col("ADA/USD")/col("ADA/EUR"))
dfjson = dfjson.withColumn("usd_eur", col("ADA/EUR")/col("ADA/USD"))
dfjson = dfjson.withColumn("usd_eur", col("ADA/EUR")/col("ADA/USD"))
dfjson = dfjson.withColumnRenamed("ADA/USD", "ada_usd")
dfjson = dfjson.withColumnRenamed("ADA/EUR", "ada_eur")


# 3. Output data: show result in the console
query = (dfjson
         .writeStream
         .outputMode("append")
         .format("console")
         .foreachBatch(persistData)
         .start())

query.awaitTermination()
