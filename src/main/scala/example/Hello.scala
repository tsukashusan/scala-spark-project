package example

import org.apache.spark.sql.SparkSession

object Hello extends Greeting with App {
  execSpark()
//  println(greeting)

  def execSparkContext(){
    val spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
    val sc = spark.sparkContext
    //sc.hadoopConfiguration.set("fs.azure.account.key.<StorageAccountName>.blob.core.windows.net", "<StorageAccountKey>")

    val lines = sc.textFile("wasbs://adftutorial@<StorageAccountName>.blob.core.windows.net/spark/inputfiles/minecraftstory.txt")
    val counts = lines.flatMap(line => line.split(" "))
                      .map(words => (words, 1))
                      .reduceByKey(_ + _)
    counts.saveAsTextFile("wasbs://adftutorial@<StorageAccountName>.blob.core.windows.net/spark/outputfilesscala/wordcount03")
    spark.stop()
  }

  def execSpark(){
    val spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.azure.account.key.<StorageAccountName>.blob.core.windows.net", "<StorageAccountKey>")

    val lines = spark.read.text("wasbs://adftutorial@<StorageAccountName>.blob.core.windows.net/spark/inputfiles/minecraftstory.txt") .rdd.map(row => row.get(0))
    val counts = lines.flatMap(line => line.asInstanceOf[String].split(' ')).map(words => (words, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("wasbs://adftutorial@<StorageAccountName>.blob.core.windows.net/spark/outputfilesscala/wordcount01")
    spark.stop()
  }
}

trait Greeting {
  lazy val greeting: String = "hello"
}
