package com.CarloConsumer.scala

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, StringType, IntegerType, StructField, StructType}
import com.databricks.spark.csv
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext._

object Consumer {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("You have to provide input_filename and output_filename as args")
      System.err.println("ABSOLUTE PATHs please....e.g. ")
      System.err.println("file:/home/training/Desktop/ScrutiniFI.csv file:/home/training/Desktop/myOutput.csv")
      System.exit(1)
    }
    val conf = new SparkConf()
      .setAppName("CarloV")
      //.setMaster("local[2]")
      //.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val filepath= args(0)
    //filepath="file:/home/training/Desktop/ScrutiniFI.csv"
    val destination_file = args(1)
    //destination_file="file:/home/training/Desktop/myout.csv"
    //val data = sc.textFile(filepath).filter(x=> !x.contains("ELETTORI")).map(x=> x.split(";").map(x=> x.trim()))

    //(DESCREGIONE,(ELETTORI,ELETTORI_M,VOTANTI,VOTANTI_M,NUMVOTISI,NUMVOTINO,NUMVOTIBIANCHI,NUMVOTINONVALIDI,NUMVOTICONTESTATI))
    //val data_region= data.map(x=>(x(0),(x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))))

    //per controllare e droppare tutti i campi che devo calcolrare che non sono digit
    def tuttiDigits(x: String) = x forall Character.isDigit


    //------>DA QUI
    val data = sc.textFile(filepath).filter(x=> !x.contains("ELETTORI")).map(x=> x.split(";").map(x=> x.trim())).keyBy(x=> (x(0),x(1),x(2))).mapValues(x=> (x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11)) )

    val regioni = data.map(x=> x._1._1).distinct().collect().filter(! _.contains("SICLIA"))

    val data_filtered = data.filter(x=> !tuttiDigits(x._2._1) | !tuttiDigits(x._2._2) | !tuttiDigits(x._2._3) | x._1._3.length()==0 | x._2._2.length==0 | ! regioni.contains(x._1._1))


    val data_cleaned = data.filter(x=> tuttiDigits(x._2._1) & tuttiDigits(x._2._2) & tuttiDigits(x._2._3) & x._1._3.length()>0 & regioni.contains(x._1._1) & x._2._2.length>0).mapValues(x=>(x._1.toDouble, x._2.toDouble, x._3.toDouble, x._4.toDouble, x._5.toDouble, x._6.toDouble, x._7.toDouble, x._8.toDouble, x._9.toDouble))

    //adesso che ho gli Int tolgo i picchi e le righe con lo 0
    val data_cleaned2 = data_cleaned.filter(x=> x._2._3==x._2._5+x._2._6+x._2._7+x._2._8+x._2._9 & x._2._3!=0 & x._2._4!=0 & x._2._5!=0 & x._2._6!=0)

    val data_filtered2=data_cleaned.filter(x=> x._2._3!=x._2._5+x._2._6+x._2._7+x._2._8+x._2._9 | x._2._3==0 | x._2._4==0 | x._2._5==0 | x._2._6==0)

    //data.filter(x=> tuttiDigits(x._2._1) & tuttiDigits(x._2._2) & tuttiDigits(x._2._3) & x._1._3.length()>0 & regioni.contains(x._1._1)).filter(x=>x._2._2=="" | x._2._3=="" | x._2._4=="" | x._2._5=="" | x._2._6=="" | x._2._7=="" | x._2._8=="" | x._2._9 =="").collect()

    //x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6, x._7+y._7, x._8+y._8, x._9+y._9
    val data_cleaned_grouped = data_cleaned2.map(x=>(x._1._1, x._2)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6, x._7+y._7, x._8+y._8, x._9+y._9))

    def round_to_decimals(x: Double, dec: Int)= BigDecimal(x).setScale(dec, BigDecimal.RoundingMode.HALF_UP).toDouble

    val decimals=4

    val output = data_cleaned_grouped.map(x=> (x._1, x._2._2.toInt, (x._2._1-x._2._2).toInt, x._2._1.toInt, round_to_decimals(x._2._3/x._2._1, decimals), round_to_decimals(x._2._5/x._2._3, decimals), round_to_decimals(x._2._6/x._2._3, decimals), round_to_decimals(x._2._7/x._2._3, decimals), round_to_decimals(x._2._8/x._2._3, decimals), round_to_decimals(x._2._9/x._2._3, decimals)))

    //output.map(x=>x.productIterator.mkString(",")).saveAsTextFile("file:/home/training/Desktop/myout.csv")

    //converto in DataFrame e scrivo utilizzando la libreria di DataBricks per i CSV
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val schema = StructType(StructField("REGIONE",StringType, false) :: StructField("Elettori Maschi",IntegerType, false) :: StructField("Elettori Femmine",IntegerType, false) :: StructField("Elettori Totali",IntegerType, false) :: StructField("Percentuali votanti", DoubleType, false) :: StructField("Percentuale voti sì", DoubleType, false) :: StructField("Percentuale voti no", DoubleType, false) :: StructField("Percentuale schede bianche", DoubleType, false) :: StructField("Percentuale non valide", DoubleType, false) :: StructField("Percentuale schede contestate", DoubleType, false) :: Nil)
    val output_row = output.map(x=>Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))

    val output_df= sqlContext.createDataFrame(output_row, schema)
    output_df.show()

    output_df.write.format("com.databricks.spark.csv").save(destination_file)

    sc.stop()

  }
}
/*
-epurare i record sporchi
-epurare i picchi e gli 0
-controllare i nomi e far emergere la SICLIA
-raggruppare per regione (eliminare PROV e COMUNE dalla chiave)
-calcolare i campi richiesti
-formattare l'output
-scrivere il csv su filesystem
-buildare con maven e provare il jar
*/





/*
package com.CarloConsumer.scala


import java.util.Date
import java.util.{Collections, Properties}

import scala.collection.JavaConverters._
import scala.sys.env
import scala.collection._

import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.consumer._
import org.apache.kafka._
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.saddle._
import org.saddle.io._


/**
  * Created by cvaccarini on 06/09/2017.
  */
object Consumer {
  def main(args: Array[String]) {
    val filename=args(0);
    val file = CsvFile(filename);
    val df = CsvParser.parse(file)
    val df2 = df.mapValues(x=>x.split(";"))
    val df3 = df2.mapValues(x=> Array(x(0),x(1).trim(),x(2).trim()))

    println(df3)



/*
    val brokers_list = sys.env.get("BROKERS_LIST")
    val topic = sys.env.get("TOPIC")
    val props = new Properties

    props.put("bootstrap.servers", "localhost:9092,localhost:9093")
    props.put("group.id", "CarloTest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    val consumer = new KafkaConsumer[String, String](props)
    //println(consumer)
    //print(consumer.listTopics().keySet())
    val topic_list = consumer.listTopics().keySet().toArray
    //se la variabile TOPIC è contenuta nella lista dei topic del broker allora viene fatto il poll per il singolo topic, else per tutti i topic accessibili dal broker
    if (false/*topic_list.contains(topic.getOrElse("no_topic_specified"))*/) {
      TopicPoll_forAllPartitions_FromBeginning(consumer, topic.get)
    } else {
      println("inizio ricorsione dei topics con IT-")
      for (x <- topic_list) {
        print (x.toString)
        if (x.toString.contains("CarloPartitions4")) {
          TopicPoll_forAllPartitions_FromBeginning(consumer, x.toString)
        }
      }
    }
*/


  }
  def TopicPoll_forAllPartitions_FromBeginning (consumer: KafkaConsumer[String, String], topic: String): Unit = {
    consumer.listTopics().get(topic).toArray().foreach(x => {
      var part = x.asInstanceOf[PartitionInfo].partition()
      var tp = new TopicPartition(x.asInstanceOf[PartitionInfo].topic(), x.asInstanceOf[PartitionInfo].partition())
      consumer.assign(Collections.singleton(tp))
      consumer.seekToBeginning(Collections.singleton(tp))
      consumer.poll(1000).asScala.foreach(y => {
        println(json_output_formatter(y, System.currentTimeMillis()))
      })
    })
  }

  def TopicPartitionPoll_fromOffset (consumer: KafkaConsumer[String, String], tp: TopicPartition, lastOff: Int): Unit = {

      consumer.assign(Collections.singleton(tp))
      consumer.seek(tp, lastOff)
      consumer.poll(1000).asScala.foreach(x=>println(json_output_formatter(x, System.currentTimeMillis())))

  }

  def json_output_formatter (y: ConsumerRecord[String, String], t: Long): String = {
    return """{"poll_timestamp": """ + t.toString + ""","topic": """"+ y.topic() + """, "offset": """+ y.offset() + """", "key": """"+ y.key() + """", "partition": """+ y.partition() + """, "value": """+ y.value() + """}"""
  }
}
*/