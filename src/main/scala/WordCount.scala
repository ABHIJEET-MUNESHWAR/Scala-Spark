/**
  * Created using IntelliJ IDEA.
  * Author:  abhijeet,
  * Date:    03/11/18,
  * Time:    7:16 PM
  */

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("src/main/resources/in.txt")

    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)

    System.out.println("Total words: " + counts.count())
    counts.saveAsTextFile("src/main/resources/word-count-output")
  }
}
