import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object AvroConsumerStructuredStreamingApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("AvroConsumerStructuredStreamingApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaBrokers = "localhost:9092"
    val kafkaTopic = "avro_df"
    val avroSchema = """
      {
        "type": "record",
        "name": "MyRecord",
        "fields": [
          {"name": "timestamp", "type": "string"},
          {"name": "index_concat", "type": "string"}
        ]
      }
    """

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .load()

    val decodedDF = kafkaStreamDF
      .select(from_avro($"value", avroSchema).as("decoded"))
      .select("decoded.*")
      .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))
      .withColumn("fecha", to_date($"timestamp"))

    val queryConsole = decodedDF.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .start()

    val queryJson = decodedDF.writeStream
      .outputMode(OutputMode.Append())
      .format("json")
      .option("path", "/home/bosonituser/Desktop/consumers/json")
      .option("checkpointLocation", "/home/bosonituser/Desktop/consumers/checkpoint")
      .start()

    queryConsole.awaitTermination()
    queryJson.awaitTermination()
    }
}

