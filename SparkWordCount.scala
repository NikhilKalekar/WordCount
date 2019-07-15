// Databricks notebook source
// Name : Nikhil Kalekar

//  Problem Statement:
// Find out all the word count from the Book of your choice.

// COMMAND ----------

val input = sc.textFile("/FileStore/tables/book.txt").map(_.toLowerCase).flatMap(x=> x.split(" ")).filter(x=>x.length>5)

// COMMAND ----------

input.collect()

// COMMAND ----------

input.count()

// COMMAND ----------

val output=input.map(x=>(x,1)).reduceByKey((x,y)=>x+y)

// COMMAND ----------

output.sortBy(-_._2).collect()

// COMMAND ----------

val kv = List((1,2),(3,4),(5,6))

// COMMAND ----------

val Rdd1 = sc.parallelize(kv)

// COMMAND ----------

Rdd1.sortBy(-_._2).collect()

// COMMAND ----------

Rdd1.sortByKey().collect()

// COMMAND ----------

Rdd1.keys.collect()

// COMMAND ----------

Rdd1.values.collect()

// COMMAND ----------

val wikiWords = sc.textFile("/FileStore/tables/text.txt").filter(_.length>0).flatMap(line=>line.split("""\W+"""))

// COMMAND ----------

wikiWords.take(10)

// COMMAND ----------

val words1 = wikiWords.filter(x=>x.length>5)

// COMMAND ----------

// val output=input.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
// getting the number of words based of the word length
val output2= words1.map(x=>(x.length,1)).reduceByKey((x,y)=>x+y)

// COMMAND ----------

output2.sortByKey().collect

// COMMAND ----------

words1.filter(x=>x.length==24).collect

// COMMAND ----------

val ZWords=words1.filter(x=>x.startsWith("z"))
val sum = ZWords.map(x=>x.length).sum
val count = ZWords.map(x=>x.length).count
sum/count

// COMMAND ----------

val charCount = words1.map(_.toLowerCase).map(x=>(x(0),x.length))

// COMMAND ----------

words1.map(x=>(x,1)).reduceByKey((x,y)=>(x+y)).sortBy(-_._2).collect

// COMMAND ----------


