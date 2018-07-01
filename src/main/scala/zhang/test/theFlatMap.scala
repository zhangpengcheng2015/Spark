package zhang.test

import org.apache.spark.{SparkConf, SparkContext}

object theFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("theFlatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.parallelize(List("hello world","hi"))
    val words = lines.flatMap(line=>line.split(" "))
    println(words.first())

  }
}
