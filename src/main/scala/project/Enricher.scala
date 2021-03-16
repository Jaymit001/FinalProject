package project

import java.time.Duration
import java.util.Properties
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object Enricher extends App {
  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "enrichedTrip")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
  consumerProperties.setProperty("schema.registry.url", "quickstart.cloudera:8081")
  consumerProperties.setProperty("AUTOFFSET_RESET_CONFIG", "earliest")

  val consumer = new KafkaConsumer[String, GenericRecord](consumerProperties)
  consumer.subscribe(List("enriched_trip").asJava)

  println("| Key | Message | Partition | Offset |")
  while(true) {
    val polledRecords = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value().toString} | ${record.partition()} | ${record.offset()} |")
      }
    }
  }

}
