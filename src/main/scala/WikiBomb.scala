import org.apache.spark.sql.SparkSession

object WikiBomb {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Input not Provided")
      System.exit(1)
    }
    val spark = SparkSession.builder.master("yarn").getOrCreate()
    val sc = spark.sparkContext
    val lines = spark.read.textFile(args(0)).rdd                                        //Read links file
    val links = lines.map(s => (s.split(":")(0), s.split(": ")(1).split(" ")))          //Format links file
    val titles = spark.read.textFile(args(1)).rdd.zipWithIndex().mapValues(x=>x+1).map(_.swap).map{case(i,title) => (i.toString,title)}   //Read titles file
    val titlesHasSurfing = titles.filter{case(k,v)=>v.toLowerCase.contains("surfing")}                                                   // Find the titles that has surfing
    val totalPages= titlesHasSurfing.count()                                         //Find the total pages that contain surfing
    val initialWebGraph = links.join(titlesHasSurfing).map(t=>(t._1, t._2._1))
    val hasRockyMountain = titles.filter(x=>x._2.equalsIgnoreCase("rocky_mountain_national_park")).keys.take(1)            //Find the id of rocky mountain national park
    val finalWebGraph = initialWebGraph.mapValues(x=> x:+hasRockyMountain.mkString(""))                   //Append rocky mountain id to initial web graph
    var ranks = finalWebGraph.mapValues(v => 1.0 / totalPages)
    for (i <- 1 to 25) {
      val tRank = finalWebGraph.join(ranks).values.flatMap {
        case (urls, rank) =>
          val outgoingLinks = urls.size
          urls.map(url => (url, rank / outgoingLinks))
      }

      ranks = tRank.reduceByKey(_ + _)
    }
    val titleRanks = titles.join(ranks).values
    val sortedOutput = titleRanks.sortBy(_._2,false).take(10)
    sc.parallelize(sortedOutput.toSeq).coalesce(1,shuffle = true).sortBy(_._2,false).saveAsTextFile(args(2))
    spark.stop()
  }
    }
