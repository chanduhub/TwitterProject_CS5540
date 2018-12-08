import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object TwitterAnalysis {
  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val sparkConf = new SparkConf().setAppName("spart_test").setMaster("local[*]")
    val sc = new SparkContext(sparkConf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    

    val textFile = sqlContext.read.json("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\tweets.txt")
    textFile.createOrReplaceTempView("twit")

    //  Query - 1.	List of 3 sports names and the count of their occurrences which gives popular sport name

    val popularSports = sqlContext.sql("select count(*),"+"CASE WHEN text like '%Football%' then 'FOOTBALL'"+"WHEN text like '%Cricket%' then 'CRICKET'"+
      "WHEN text like '%Basketball%' then 'BASKETBALL'"+ "END AS game from twit group by game")

    // "select text,count(1) where text like '%Football%' or '%Cricket%' or '%basketball%' from twit group by text"
    var teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q1")
    if(teFile.exists())
    {
      val dirs = teFile.list()
      for(i <- dirs)
      {
        var currentFile = new File(teFile.getPath(),i)


        currentFile.delete()
      }
      teFile.delete()
    }
    popularSports.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q1")

    // Query 2 - Favourite sports of people speaking different languages
    val languague = sqlContext.sql("select user.lang,count(*) as numoftweets,"+"CASE WHEN text like '%Football%' then 'FOOTBALL'"+"WHEN text like '%Cricket%' then 'CRICKET'"+
          "WHEN text like '%Basketball%' then 'BASKETBALL' ELSE 'NogivenSport'"+ "END AS game from twit where text is not null group by game,user.lang order by game")

    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q2")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    languague.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q2")


   // Query 3 - More tweets coming from locations
    val moretweetLoc = sqlContext.sql("select user.location as location,count(*) as numoftweets  from twit where user.location is not null group by user.location order by count(1) desc limit 10")
    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q3")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    moretweetLoc.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q3")



    //Query 4- Users who enabled geolocation

    val geoEnabled = sqlContext.sql("select user.geo_enabled as location,count(*) as numofusers  from twit where user.geo_enabled is not null group by user.geo_enabled")
    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q4")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    geoEnabled.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q4")

      //Extrating Hashtags from the tweet data
      val hashTags = sqlContext.sql("select entities.hashtags from twit where entities.hashtags is not null")

      val hashtags = hashTags.select(org.apache.spark.sql.functions.explode(hashTags.col("hashtags")))
      val hashtagtabe =hashtags.select("col.text")

      hashtagtabe.createOrReplaceTempView("hashtags")

      // Query 5- Top hashtags
      val topHashtags = sqlContext.sql("select text as hashTag,count(*) as numofoccur from hashtags group by text order by count(1) desc limit 10")
      teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q5")
      if(teFile.exists())
      {
        val entries = teFile.list()
        for(s <- entries)
        {
          var currentFile = new File(teFile.getPath(),s)
          currentFile.delete()
        }
        teFile.delete()
      }
      topHashtags.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q5")




      // Query 6 - Football related, which is famous among Liverpool, RealMadrid and Barcelona
      val football = sqlContext.sql("select text,count(*) as total from hashtags where text like '%Liverpool%' or text like '%RealMadrid%' or text like '%Barcelona%' group by text order by count(1) desc limit 10")
      teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q6")
      if(teFile.exists())
      {
        val entries = teFile.list()
        for(s <- entries)
        {
          var currentFile = new File(teFile.getPath(),s)
          currentFile.delete()
        }
        teFile.delete()
      }
      football.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q6")



    // Query 7 - Top Friends
    val topfriends= sqlContext.sql("select user.name,user.friends_count  from twit order by user.friends_count desc LIMIT 10")
    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q7")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    topfriends.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q7")



    // Query 8 - Top 10 Users Who are actively liking tweets.
    val likingUsers = sqlContext.sql("select user.name,user.favourites_count from twit order by user.favourites_count desc limit 10")
    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q8")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    likingUsers.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q8")


    //Query 9 - Top 10 Users Who are actively liking tweets.
    val topSources = sqlContext.sql("select count(*) as total,"+
      "CASE WHEN source like '%iphone%' then 'IPHONE'"+"WHEN source like '%android%' then 'android'"+ "WHEN source like '%Web%' then 'Web'"+ "END AS source " +
      "from twit group by source order by count(1) desc limit 3")
    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q9")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    topSources.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q9")



    // Query 10 - Location of Tweets on Map
    val geomap=sqlContext.sql("SELECT coordinates.coordinates[0] as lat,coordinates.coordinates[1] as lon FROM twit where coordinates is not null")
    teFile = new File("C:\\Users\\chand\\Documents\\PB Project\\tweets data\\TestData\\Q10")
    if(teFile.exists())
    {
      val entries = teFile.list()
      for(s <- entries)
      {
        var currentFile = new File(teFile.getPath(),s)
        currentFile.delete()
      }
      teFile.delete()
    }
    geomap.coalesce(1).write.json("C:/Users/chand/Documents/PB Project/tweets data/TestData/Q10")


  }

}
