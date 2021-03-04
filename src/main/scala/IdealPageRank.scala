import org.apache.spark.sql.SparkSession

object IdealPageRank{

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Input not Provided")
      System.exit(1)
  }
    // Creating a sprak session
    val spark = SparkSession.builder.master("yarn").getOrCreate()
    val sc = spark.sparkContext
    
    // Reading in the wikipedia links file which is of the format- "from1 : to3, to11, to100"
    val lines = spark.read.textFile(args(0)).rdd
    
    // Reading corresponding titles and adding index to them starting from 1
    val titles = spark.read.textFile(args(1)).rdd.zipWithIndex().mapValues(x=>x+1).map(_.swap).map{case(i,title) => (i.toString,title)}
    
    // Count the number of pages
    val totalPages = titles.count()
    
    //Split the from and to links and put into key and value form
    val links = lines.map(s => (s.split(":")(0), s.split(": ")(1).split(" ")))
    // Initialize ranks to all the links
    var ranks = links.mapValues(v => 1.0 / totalPages)
    //Iteratively calculate the pageRank
    for (i <- 1 to 25) {
      val tRank = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val outgoingLinks = urls.size
          urls.map(url => (url, rank / outgoingLinks))
      }

      ranks = tRank.reduceByKey(_ + _)
    }
    // Join the titles with the ranks of the links
    val titleRanks = titles.join(ranks).values
    //Sort it in decreasing order and take the top 10
    val sortedOutput = titleRanks.sortBy(_._2,false).take(10)
    //Save it as a text file
    sc.parallelize(sortedOutput.toSeq).coalesce(1,shuffle = true).sortBy(_._2,false).saveAsTextFile(args(2))
    spark.stop()
  }
}
