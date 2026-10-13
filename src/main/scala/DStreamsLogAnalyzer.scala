import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsLogAnalyzer {
  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999

    val conf = new SparkConf()
      .setAppName("DStreams Log Analyzer")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Read log lines from a TCP socket
    val logLines = ssc.socketTextStream(host, port)

    // flatMap: split each line into words
    val words = logLines.flatMap(_.split("\\s+"))

    // map: convert each word into a (word, count) pair
    val wordPairs = words.map(word => (word, 1))

    // filter: keep only ERROR messages
    val errorWords = wordPairs.filter {
      case (word, _) => word.equalsIgnoreCase("ERROR")
    }

    // Count ERROR messages in each 5-second micro-batch
    val errorCounts = errorWords.reduceByKey(_ + _)

    errorCounts.foreachRDD { rdd =>
      val batchTime = java.time.LocalTime.now()
      val count = rdd.map(_._2).fold(0)(_ + _)
      println(s"[$batchTime] ERROR count in this micro-batch: $count")
    }

    println(s"Listening for logs on $host:$port")
    println("Batch interval: 5 seconds")

    ssc.start()
    ssc.awaitTermination()
  }
}
