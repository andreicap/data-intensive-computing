package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {

  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("DROP TABLE IF EXISTS avg_keyspace.avg;")
    session.execute("CREATE TABLE  IF NOT EXISTS avg_keyspace.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setAppName("avg").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))    
    ssc.checkpoint("./checkpoint")

    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

    val topics = Set("avg")
    
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

    val values = messages.map{case (key, value) => value.split(',')}    
    val pairs = values.map(record => (record(0), record(1).toDouble))
    
    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
       // get the observations of this letter
       var oldState: (Double, Int) = state.getOption.getOrElse[(Double, Int)]((0.0, 0))
       val sum = oldState._1 * oldState._2
       val newCount = oldState._2 + 1
       if (value.isEmpty) {
        state.update(oldState)
    }
    else  {
        val newState: (Double, Int) = ((sum + value.get) / newCount, newCount)
        state.update(newState)
    }
    val output = (key, state.getOption.get._1)
    output
}

val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

// store the result in Cassandra
stateDstream.saveToCassandra("avg_keyspace", "avg", SomeColumns("word", "count"))

ssc.start()
ssc.awaitTermination()
}
}










