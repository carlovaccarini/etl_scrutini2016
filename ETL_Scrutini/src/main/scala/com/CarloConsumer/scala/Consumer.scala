package com.CarloConsumer.scala

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, StringType, IntegerType, StructField, StructType}
import com.databricks.spark.csv
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SparkSession._

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
    val destination_file = args(1)

    //per controllare e droppare tutti i campi che devo calcolrare che non sono digit
    def tuttiDigits(x: String) = x forall Character.isDigit

    val data = sc.textFile(filepath).filter(x=> !x.contains("ELETTORI")).map(x=> x.split(";").map(x=> x.trim())).keyBy(x=> (x(0),x(1),x(2))).mapValues(x=> (x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11)) )

    val regioni = data.map(x=> x._1._1).distinct().collect().filter(! _.contains("SICLIA"))

    val data_filtered = data.filter(x=> !tuttiDigits(x._2._1) | !tuttiDigits(x._2._2) | !tuttiDigits(x._2._3) | x._1._3.length()==0 | x._2._2.length==0 | ! regioni.contains(x._1._1))


    val data_cleaned = data.filter(x=> tuttiDigits(x._2._1) & tuttiDigits(x._2._2) & tuttiDigits(x._2._3) & x._1._3.length()>0 & regioni.contains(x._1._1) & x._2._2.length>0).mapValues(x=>(x._1.toDouble, x._2.toDouble, x._3.toDouble, x._4.toDouble, x._5.toDouble, x._6.toDouble, x._7.toDouble, x._8.toDouble, x._9.toDouble))

    //adesso che ho gli Int tolgo i picchi e le righe con lo 0
    val data_cleaned2 = data_cleaned.filter(x=> x._2._3==x._2._5+x._2._6+x._2._7+x._2._8+x._2._9 & x._2._3!=0 & x._2._4!=0 & x._2._5!=0 & x._2._6!=0)

    val data_filtered2=data_cleaned.filter(x=> x._2._3!=x._2._5+x._2._6+x._2._7+x._2._8+x._2._9 | x._2._3==0 | x._2._4==0 | x._2._5==0 | x._2._6==0)
    val data_cleaned_grouped = data_cleaned2.map(x=>(x._1._1, x._2)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6, x._7+y._7, x._8+y._8, x._9+y._9))

    def round_to_decimals(x: Double, dec: Int)= BigDecimal(x).setScale(dec, BigDecimal.RoundingMode.HALF_UP).toDouble

    val decimals=4

    val output = data_cleaned_grouped.map(x=> (x._1, x._2._2.toInt, (x._2._1-x._2._2).toInt, x._2._1.toInt, round_to_decimals(x._2._3/x._2._1, decimals), round_to_decimals(x._2._5/x._2._3, decimals), round_to_decimals(x._2._6/x._2._3, decimals), round_to_decimals(x._2._7/x._2._3, decimals), round_to_decimals(x._2._8/x._2._3, decimals), round_to_decimals(x._2._9/x._2._3, decimals)))
    
    //converto in DataFrame e scrivo utilizzando la libreria di DataBricks per i CSV
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val spark = SparkSession.builder().getOrCreate()
    val schema = StructType(StructField("REGIONE",StringType, false) :: 
                            StructField("Elettori Maschi",IntegerType, false) :: 
                            StructField("Elettori Femmine",IntegerType, false) :: 
                            StructField("Elettori Totali",IntegerType, false) :: 
                            StructField("Percentuali votanti", DoubleType, false) :: 
                            StructField("Percentuale voti sì", DoubleType, false) :: 
                            StructField("Percentuale voti no", DoubleType, false) :: 
                            StructField("Percentuale schede bianche", DoubleType, false) :: 
                            StructField("Percentuale non valide", DoubleType, false) :: 
                            StructField("Percentuale schede contestate", DoubleType, false) :: Nil)
    val output_row = output.map(x=>Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))

    //val output_df= sqlContext.createDataFrame(output_row, schema)
     val output_df= spark.createDataFrame(output_row, schema): DataFrame
    output_df.show()

    output_df.write.format("com.databricks.spark.csv").save(destination_file)
    
    spark.stop()
    sc.stop()

  }
}
