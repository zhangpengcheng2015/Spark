package zhang.test

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)
    val textRdd = sc.textFile("D:\\Documents\\Develope\\Spark\\src\\main\\resources\\The_Man_of_Property.txt")
    textRdd.flatMap(line => line.split(" ")).map(line => (line, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(20).foreach(println)
  }
}
