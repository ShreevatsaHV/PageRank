// Databricks notebook source
import sys.process._

// COMMAND ----------

"wget -P /tmp http://www.utdallas.edu/~axn112530/cs6350/data/PRData/webcrawl" !

// COMMAND ----------

val pages = sc.textFile("file:/tmp/webcrawl")

// COMMAND ----------

val pagemap = pages.map(x => ((x.split("\t"))(0), (x.split("\t"))(1), (x.split("\t"))(2)))

// COMMAND ----------

val pagemaplinks = pages.map(x => ((x.split("\t"))(0), (x.split("\t"))(2)))

// COMMAND ----------

var links1=pagemap.map(x=>(x._1,(x._2.toDouble,x._3.filterNot(c => c  == '{' || c == '}').split(",")))).distinct().groupByKey().mapValues{case (iter)=> iter.toArray}.mapValues{case fields =>fields(0)}

// COMMAND ----------

for(i<-1 to 100){
val newmap = links1.mapValues{case(rank,urls)=>
  val size = urls.size
  urls.map(url=>(url,rank/size))};
val reduced = newmap.flatMap(x=> (x._2));
//val fil = reduced.filter(x=>x._1 != """""");
//val fil1 = fil.filter(x=>x._1 != "{}");
val red = reduced.reduceByKey(_+_);
val bracket = red.map(x=>(x._1.filterNot(c => c  == '(' || c == ')'),x._2.toDouble));
links1 = bracket.join(pagemaplinks).mapValues(x=>(x._1, x._2.filterNot(c => c  == '{' || c == '}').split(","))).distinct().groupByKey().mapValues{case (iter)=> iter.toArray}.mapValues{case fields =>fields(0)};
}

// COMMAND ----------

val output = links1.sortBy{case(x:String,(y:Double,z:Array[String])) => -y }.collect()

// COMMAND ----------

// Output without rounding to 3 decimal places
output.foreach(tup => println(tup._1,tup._2))

// COMMAND ----------

var links2=pagemap.map(x=>(x._1,(x._2.toDouble,x._3.filterNot(c => c  == '{' || c == '}').split(",")))).distinct().groupByKey().mapValues{case (iter)=> iter.toArray}.mapValues{case fields =>fields(0)}

// COMMAND ----------

for(i<-1 to 100){
val newmap1 = links2.mapValues{case(rank1,urls1)=>
  val size1 = urls1.size
  urls1.map(url1=>(url1,BigDecimal(rank1/size1).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble))}; 
  //BigDecimal(1.23456789).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
val reduced1 = newmap1.flatMap(x=> (x._2));
//val fil = reduced.filter(x=>x._1 != """""");
//val fil1 = fil.filter(x=>x._1 != "{}");
val red1 = reduced1.reduceByKey(_+_);
val bracket1 = red1.map(x=>(x._1.filterNot(c => c  == '(' || c == ')'),x._2.toDouble));
links2 = bracket1.join(pagemaplinks).mapValues(x=>(x._1, x._2.filterNot(c => c  == '{' || c == '}').split(","))).distinct().groupByKey().mapValues{case (iter)=> iter.toArray}.mapValues{case fields =>fields(0)};
}

// COMMAND ----------

val output1 = links2.sortBy{case(x:String,(y:Double,z:Array[String])) => -y }.collect()

// COMMAND ----------

// output with rounding to 3 decimal places
output1.foreach(tup => println(tup._1,tup._2))

// COMMAND ----------


