package com.CarloConsumer.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, StringType, IntegerType, StructField, StructType}
import com.databricks.spark.csv
import org.apache.spark.sql.Row
/*
object spark_scala_etl_scrutini {
   def main(args: Array[String]) {

val sc = new SparkContext()

val filepath=args(0)
//filepath="file:/home/training/Desktop/ScrutiniFI.csv"
val destination_file = args(1)
//destination_file="file:/home/training/Desktop/myout.csv"
val data = sc.textFile(filepath).filter(x=> !x.contains("ELETTORI")).map(x=> x.split(";").map(x=> x.trim()))

//(DESCREGIONE,(ELETTORI,ELETTORI_M,VOTANTI,VOTANTI_M,NUMVOTISI,NUMVOTINO,NUMVOTIBIANCHI,NUMVOTINONVALIDI,NUMVOTICONTESTATI))
val data_region=data.map(x=>(x(0),(x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))))

//per controllare e droppare tutti i campi che devo calcolrare che non sono digit
def tuttiDigits(x: String) = x forall Character.isDigit

//prova.filter(x=> tuttiDigits(x._2)).map(x=>(x._1,x._2.toDouble))
//prova.filter(x=> !tuttiDigits(x._2)).map(x=>(x._1,x._2))


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
*/