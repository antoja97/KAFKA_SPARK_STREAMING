import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.avro.generic.GenericRecord
import java.util.Properties

object AvroProcessingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProducerAvroApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Definir el esquema JSON
    val schema = new StructType()
      .add(StructField("timestamp", StringType, nullable = false))
      .add(StructField("index_concat", StringType, nullable = false))

    // Configurar propiedades para el productor de Kafka
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    // Crear un productor de Kafka
    val producer = new KafkaProducer[String, Array[Byte]](props)

    val topic = "avro_df"  // Cambia el nombre del topic según tus necesidades

    // Simular la generación de datos y envío a Kafka
    val numRecords = 100  // Número de registros a generar
    for (i <- 1 to numRecords) {
      val timestamp = System.currentTimeMillis().toString
      val indexConcat = s"index_$i"

      // Crear DataFrame con una fila
      val data = Seq((timestamp, indexConcat))
      val rowRDD = spark.sparkContext.parallelize(data).map { case (ts, idx) => Row(ts, idx) }
      val df = spark.createDataFrame(rowRDD, schema)

      // Convertir DataFrame a formato Avro
      val avroDf = df.select(to_avro(struct(df.columns.map(col): _*)).as("value"))

      // Enviar mensaje Avro al topic de Kafka
      val avroBytes = avroDf.first().getAs[Array[Byte]]("value")
      val record = new ProducerRecord[String, Array[Byte]](topic, avroBytes)
      producer.send(record)
      
      println(s"Mensaje enviado al topic '$topic': $timestamp - $indexConcat")
    }

    producer.close()
    spark.stop()
  }
}

