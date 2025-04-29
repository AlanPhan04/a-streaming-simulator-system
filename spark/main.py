from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, StructField
from pyspark.sql.functions import *

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ProcessData").getOrCreate()

    ####### DEFINE SCHEMA FOR DATA RECORD #######
    air_schema = StructType([
        StructField("station", StringType()),
        StructField("time", StringType()),
        StructField("temperature", DoubleType()),
        StructField("moisture", DoubleType()),
        StructField("light", DoubleType()),
        StructField("totalRainfall", DoubleType()),
        StructField("rainfall", DoubleType()),
        StructField("windDirection", DoubleType()),
        StructField("pm2dot5", DoubleType()),
        StructField("pm10", DoubleType()),
        StructField("CO", DoubleType()),
        StructField("NOx", DoubleType()),
        StructField("SO2", DoubleType())
    ])

    earth_schema = StructType([
        StructField("station", StringType()),
        StructField("time", StringType()),
        StructField("moisture", DoubleType()),
        StructField("temperature", DoubleType()),
        StructField("pH", DoubleType()),
        StructField("waterRoot", DoubleType()),
        StructField("waterLeaf", DoubleType()),
        StructField("waterLevel", DoubleType()),
        StructField("voltage", DoubleType())
    ])

    water_schema = StructType([
        StructField("station", StringType()),
        StructField("time", StringType()),
        StructField("pH", DoubleType()),
        StructField("DO", DoubleType()),
        StructField("temperature", DoubleType()),
        StructField("salinity", DoubleType())
    ])

    ####### CREATE DATASTREAM FOR READING VALUE OF EACH DATA TYPE #######
    earthData = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
        .option("subscribe", "earth")\
        .load()\
        .select(from_json(col("value").cast("string"), earth_schema).alias("earth"))


    airData = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "air") \
        .load() \
        .select(from_json(col("value").cast("string"), air_schema).alias("air"))


    waterData = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "water") \
        .load() \
        .select(from_json(col("value").cast("string"), water_schema).alias("water"))

    ####### DATA PROCESSING BY TAKING VALUES OF REPORTED RECORD WITH 5-MINUTE AVERAGE #######
    earthData = earthData.withColumn(
        "timestamp", to_timestamp(col("earth.time"), "yyyy-MM-dd'T'HH:mm:ss")
    ).select("earth.*", "timestamp")

    airData = airData.withColumn(
        "timestamp", to_timestamp(col("air.time"), "yyyy-MM-dd'T'HH:mm:ss")
    ).select("air.*", "timestamp")

    waterData = waterData.withColumn(
        "timestamp", to_timestamp(col("water.time"), "yyyy-MM-dd'T'HH:mm:ss")
    ).select("water.*", "timestamp")

    earthAggregated = earthData.withWatermark("timestamp", "10 minutes")\
        .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("station")
    ).agg(
        avg("moisture").alias("moisture"),
        avg("temperature").alias("temperature"),
        avg("pH").alias("pH"),
        avg("waterRoot").alias("waterRoot"),
        avg("waterLeaf").alias("waterLeaf"),
        avg("waterLevel").alias("waterLevel"),
        avg("voltage").alias("voltage")
    )

    earthAggregated = earthAggregated.select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        "station",
        "moisture",
        "temperature",
        "pH",
        "waterRoot",
        "waterLeaf",
        "waterLevel",
        "voltage"
    )

    airAggregated = airData.withWatermark("timestamp", "10 minutes").groupBy(
        window(col("timestamp"), "5 minutes"),
        col("station")
    ).agg(
        avg("temperature").alias("temperature"),
        avg("moisture").alias("moisture"),
        avg("light").alias("light"),
        avg("totalRainfall").alias("totalRainfall"),
        avg("rainfall").alias("rainfall"),
        avg("windDirection").alias("windDirection"),
        avg("pm2dot5").alias("pm2dot5"),
        avg("pm10").alias("pm10"),
        avg("CO").alias("CO"),
        avg("NOx").alias("NOx"),
        avg("SO2").alias("SO2")
    )

    airAggregated = airAggregated.select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        "station",
        "temperature",
        "moisture",
        "light",
        "totalRainfall",
        "rainfall",
        "windDirection",
        "pm2dot5",
        "pm10",
        "CO",
        "NOx",
        "SO2"
    )

    waterAggregated = waterData.withWatermark("timestamp", "10 minutes").groupBy(
        window(col("timestamp"), "5 minutes"),
        col("station")
    ).agg(
        avg("pH").alias("pH"),
        avg("DO").alias("DO"),
        avg("temperature").alias("temperature"),
        avg("salinity").alias("salinity")
    )

    waterAggregated = waterAggregated.select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        "station",
        "pH",
        "DO",
        "temperature",
        "salinity"
    )

    ####### MERGE THE THREE DATA TYPE STREAMS INTO ONE WITH MATCHING TIME INTERVAL #######
    from pyspark.sql.functions import col

    queryJoin = earthAggregated.alias("e").join(
        airAggregated.alias("a"),
        col("e.start_time") == col("a.start_time")
    ).join(
        waterAggregated.alias("w"),
        col("e.start_time") == col("w.start_time")
    ).select(
        # Shared columns
        col("e.start_time").alias("start_time"),
        col("e.end_time").alias("end_time"),

        # Station identifiers
        col("e.station").alias("earth_station"),
        col("a.station").alias("air_station"),
        col("w.station").alias("water_station"),

        # Earth data
        col("e.moisture").alias("earth_moisture"),
        col("e.temperature").alias("earth_temperature"),
        col("e.pH").alias("earth_pH"),
        col("e.waterRoot").alias("earth_waterRoot"),
        col("e.waterLeaf").alias("earth_waterLeaf"),
        col("e.waterLevel").alias("earth_waterLevel"),
        col("e.voltage").alias("earth_voltage"),

        # Air data
        col("a.temperature").alias("air_temperature"),
        col("a.moisture").alias("air_moisture"),
        col("a.light").alias("air_light"),
        col("a.totalRainfall").alias("air_totalRainfall"),
        col("a.rainfall").alias("air_rainfall"),
        col("a.windDirection").alias("air_windDirection"),
        col("a.pm2dot5").alias("air_pm2dot5"),
        col("a.pm10").alias("air_pm10"),
        col("a.CO").alias("air_CO"),
        col("a.NOx").alias("air_NOx"),
        col("a.SO2").alias("air_SO2"),

        # Water data
        col("w.pH").alias("water_pH"),
        col("w.DO").alias("water_DO"),
        col("w.temperature").alias("water_temperature"),
        col("w.salinity").alias("water_salinity")
    )


    ##### Store the data of each type also their unified version in the database #####
    def earth_table(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/") \
            .option("dbtable", "Earth") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    earthQuery = earthAggregated.writeStream \
        .foreachBatch(earth_table) \
        .outputMode("append") \
        .start() \


    def air_table(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/") \
            .option("dbtable", "Air") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()


    airQuery = airAggregated.writeStream \
        .foreachBatch(air_table) \
        .outputMode("append") \
        .start() \

    def water_table(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/") \
            .option("dbtable", "Water") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()


    waterQuery = waterAggregated.writeStream \
        .foreachBatch(water_table) \
        .outputMode("append") \
        .start() \

    def unified_table(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/") \
            .option("dbtable", "UnifiedData") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()


    queryJoin = queryJoin.writeStream \
        .foreachBatch(unified_table) \
        .outputMode("append") \
        .start() \

    earthQuery.awaitTermination()
    airQuery.awaitTermination()
    waterQuery.awaitTermination()
    queryJoin.awaitTermination()

