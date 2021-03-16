package project

import java.util.Properties
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object AvroProducer extends App with Main {
  def Prod(args: Array[String]): Unit = {
    val topicName = "winter2020_jay_trip"

    val prodProperties = new Properties()
    prodProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092"
    )
    prodProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
    )
    prodProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )

    val producer = new KafkaProducer[Int, String](prodProperties)

    val inputStream: FSDataInputStream = fs
      .open(new Path("/home/Jay/Downloads/BixiMontrealRentals2014/100_trips.csv/100_trips.csv"))

    var i = 1
    val data = scala.io.Source.fromInputStream(inputStream).getLines()
      .drop(1).mkString("\n")
      .split("\n")
      .foreach(line => {
        println(line)
        producer.send(new ProducerRecord[Int, String](topicName, i, line))
        i = i + 1
      })
    producer.flush()
  }

}
