import org.apache.spark.sql.SparkSession

object TaxationPageRank{

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Input not Provided")
      System.exit(1)
    }

    val spark = SparkSession.builder.master("yarn").getOrCreate()
    val sc = spark.sparkContext
    val lines = spark.read.textFile(args(0)).rdd
    val titles = spark.read.textFile(args(1)).rdd.zipWithIndex().mapValues(x=>x+1).map(_.swap).map{case(i,title) => (i.toString,title)}
    val totalPages = titles.count()
    val links = lines.map(s => (s.split(":")(0), s.split(": ")(1).split(" ")))
    var ranks = links.mapValues(v => 1.0 / totalPages)

    for (i <- 1 to 25) {
      val tRank = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val outgoingLinks = urls.size
          urls.map(url => (url, rank / outgoingLinks))
      }
      // Add taxation to the ranks
      ranks = tRank.reduceByKey(_ + _).mapValues((0.15/totalPages) + 0.85 * _)
    }
    val titleRanks = titles.join(ranks).values
    val sortedOutput = titleRanks.sortBy(_._2,false).take(10)
    sc.parallelize(sortedOutput.toSeq).coalesce(1,shuffle = true).sortBy(_._2,false).saveAsTextFile(args(2))
    spark.stop()
  }
}
