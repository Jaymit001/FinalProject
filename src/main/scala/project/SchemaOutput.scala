package project

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import scala.collection.JavaConverters._

object SchemaOutput extends App {

  val regClient = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 3)

  regClient.getAllSubjects.asScala.foreach(println)

  val metadata = regClient.getSchemaMetadata("enriched_jaytrip-value", 1)
  val schemaId = metadata.getId

  val schema = regClient.getByID(schemaId)
  println(schema.toString(true))

}
