package hellosparksql;

//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions
//import org.apache.spark.sql.DataFrameReader

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future

object Runner {

  case class myPerson(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    //initialize a SparkSession, by convention called spark
    //SparkSession is the entrypoint for a Spark application using Spark SQL
    // it's new in Spark 2 + unifies older context objects.
    //SparkSession is different from SparkContext in that we can have multiple sessions
    // in the same runtime, where we only wanted 1 SparkContext per application.

    val spark = SparkSession
      .builder()
      .appName("Hello Spark SQL")
      .master("local[4]")
      .getOrCreate()

              //we want to always add an import here, it enables some syntax and code generation:
              // if you run into mysterious errors with what should be working code, check to make sure this import exists
              import spark.implicits._

              spark.sparkContext.setLogLevel("WARN")

              //helloSparkSql(spark)

              //helloTweetStream(spark)

              helloMySQL(spark)

    spark.stop()

  }

  // ==========================================================================================

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream",
      linesPerFile: Int = 1000
  ) = {
    //a decent chunk of boilerplate -- from twitter docs/tutorial
    //sets up the request we're going to be sending to Twitter
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()

    /*
    val twitterURL  = "https://api.twitter.com/2/users/by?usernames=twitter,twitterdev,twitterapi,twitternyc,twittersf";
    val qString1    = "?user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld";
    val qString2    = "&expansions=pinned_tweet_id"
    val qString3    = "&tweet.fields=attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,possibly_sensitive,promoted_metrics,referenced_tweets,source,text,withheld";
    val twitterURI  = twitterURL+qString1+qString2+qString3;
    */
    
    val twitterURL = "https://api.twitter.com/2/tweets/sample/stream";
    val queryString = "?tweet.fields=geo&expansions=geo.place_id";
    val twitterURI = twitterURL + queryString;

    val uriBuilder: URIBuilder = new URIBuilder( twitterURI );
    
    val httpGet = new HttpGet(uriBuilder.build())
    //set up the authorization for this request, using our bearer token
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      //initial filewriter, replaced every linesPerFile
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }

  // ==========================================================================================

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    //grab a bearer token from the environment
    //never hardcode your tokens (never just put them as a string in your code)
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    //writes all the tweets from twitter's stream into a directory
    // by default hits the sampled stream and uses "twitterstream" as the directory
    // We'll run it in the background using a Future:
    // We're not saving a reference to this future or providing a callback function
    // we just start it running in the background and forget about it.
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir( bearerToken )
    }

    //Here we're just going to wait until a file appears in our twitterstream directory
    // or until some reasonable amount of time has passed (30s)
    
    var start = System.currentTimeMillis()
    var filesFoundInDir = false

    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 60000) {
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }

    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 60 seconds.  Exiting..")
      System.exit(1)
    }

    //We're going to start with a static DF
    // both to demo it, and to infer the schema
    // streaming dataframes can't infer schema

    val staticDf = spark.read
      .option("multiline","true")
      .option("inferSchema","true")
      .json("twitterstream")

    //val staticDf = spark.read.json("twitterstream");
      staticDf.printSchema();

      //staticDF.createOrReplaceTempView("TweetTable");  // create database

    //val tweetText = spark.sql("SELECT data.text FROM TweetTable")
    
    //val results = tweetText.collect()
        //results.foreach(println)

    //streamDf is a stream, using *Structured Streaming*
    //val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    //Display placenames as tweets occur.  Have to deal with a good chunk of nested data
    // Writing a case class and using DataSets would be more initial investment, but
    // would make writing queries like this much easier!
    /*
    streamDf
      .filter(!functions.isnull($"includes.places"))
      .select(functions.element_at($"includes.places", 1)("full_name").as("Place"), ($"data.text").as("Tweet"))
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
    */
    //Example just getting the text:
    // streamDf
    //   .select($"data.text")
    //   .writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .start()
    //   .awaitTermination()

    //Most used twitter handles, aggregated over time:
    
    // regex to extract twitter handles
    //val pattern = ".*(@\\w+)\\s+.*".r

    // streamDf
    //   .select($"data.text")
    //   .as[String]
    //   .flatMap(text => {text match {
    //     case pattern(handle) => {Some(handle)}
    //     case notFound => None
    //   }})
    //   .groupBy("value")
    //   .count()
    //   .sort(functions.desc("count"))
    //   .writeStream
    //   .outputMode("complete")
    //   .format("console")
    //   .start()
    //   .awaitTermination()
  
  }

  
  // ==========================================================================================

  def helloMySQL(spark: SparkSession): Unit = {
  
      import spark.implicits._

        // load each line of the source data into a Dataset
        /*
      val schemaPeople = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/friends.csv")
      .as[myPerson]
      */
      /*
      val schemaPeople = spark.read
      .option("multiline","true")
      .option("inferSchema","true")
      .json("persons.json")
      */

//.as[myPerson]
// schemaPeople.printSchema()
// schemaPeople.show()

    // OBTAIN TWITTER DATA IN THE BACKGROUND ================
    import scala.concurrent.ExecutionContext.Implicits.global
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))
    Future {
      tweetStreamToDir( bearerToken )
    }

    // MOVE TWITTER DATA TO OUR DIRECTORY ===================
    var start = System.currentTimeMillis()
    var filesFoundInDir = false

    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 60000) {
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }

    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 60 seconds.  Exiting..")
      System.exit(1)
    }

    // READ TWITTER DATA ====================================
      val listTweets = spark.read.json("twitterstream")
          listTweets.createOrReplaceTempView("TweetTable") // create database

    // MINE TWITTER DATA ====================================

      var positiveHashTags = Array("#Yay", "#LOVE", "#Dboss", "heart");
      analize_hashtags(spark, positiveHashTags);

      var negativeHashTags = Array("#Traitors", "abandon", "cry", "Trump");
      analize_hashtags(spark, negativeHashTags);

      /*
      var twtText  = spark.sql(" CRAVITY%'");
          twtText.printSchema(); // view the schema of the twtText dataframe
          twtText.show();        // see all data in the df in a tabular format

          println( "Done." );
     */ 
    // DISPLAY THE RESULTS ==================================
    //val results = twtText.collect()
    //results.foreach(println)
   
  }

  // function to count the number of tweets each hashtag appears in =================================

  def analize_hashtags(spark: SparkSession, arr: Array[String]): Unit = {
   
    var twtSQL = "";
    var htCount:Int = 0;

    // READ TWITTER DATA ---------------
    val listTweets = spark.read.json("twitterstream")
        listTweets.createOrReplaceTempView("TweetTable") // create database

    // COUNT THE HASHTAGS --------------
    println("Analyzing the Data --------")
    for (i <- 0 to arr.length - 1) {
        
        twtSQL = "SELECT Count(*) AS Count FROM TweetTable WHERE data.text LIKE '%" + arr(i) + "%'";

        val twtCount = spark.sql( twtSQL );
        println( arr(i) + " : " + twtCount.head().get(0) );
        
    }

  }

  // ==========================================================================================

  def helloSparkSql(spark: SparkSession): Unit = {
    //break intro demo off into a method, add import here as well.
    import spark.implicits._

    //From the Spark Session, we can create a DataFrame.  We're going to read from a JSON file to do so
    // though you can also create DFs from RDDs, parquet files, Hive tables, SQL tables, ... spark inputs
    // Spark will infer the schema for our json file, though we can also specify.
    val df = spark.read.option("multiline", "true").json("persons.json")

    df.show()

    df.printSchema()

    //select columns by name, in a few ways:
    df.select("name").show()
    df.select("name", "age").show()

    //use the $ syntax to select columns
    // this gets us Column objects instead of just strings.  These can be used in expressions
    df.select($"name", $"age").show()

    df.select($"name", ($"age" + 10).as("age plus ten"))
      .show() // adds 10 to every value in the age column

    //fruit csv interlude
    val dfCsv = spark.read.option("header", "true").csv("fruits.csv")

    dfCsv.show()

    dfCsv.printSchema()

    //access nested values using a "."

    df.select("name.first", "name.last").show()

    //we can operate on DataFrames:
    // group by eye color and count each group
    df.groupBy("eyeColor").count().show()

    // filter based on age:
    df.filter($"age" > 30).show()

    // instead of the $ syntax, we can use the identifier for the dataframe:
    df.filter(df("age") > 30).show() // this gives the same output

    // The $ is often preferred so we don't repeat the df identifier many times, ex:
    //adamsNeatDf.filter(adamsNeatDf("num") < 200)

    //There are many built in functions, we can access some on dfs and some on a functions object that we import
    // these include scalar and aggregate functions, similar to SQL.
    df.select(functions.exp($"age")).show() // exponentiation
    df.select(functions.round($"age", -1)).show() // round age to nearest 10

    //average age by eye color for people with a first name less than 6 characters in length:
    val demoQuery = df
      .filter(functions.length($"name.first") < 6)
      .groupBy($"eyeColor")
      .agg(functions.avg($"age"))

    demoQuery.show()

    //knowing where to find functionality requires exploring the API
    // typically scalar/aggregate functions from SQL can be found on the functions object

    //2 more lines of code to run before lunch:
    // explain catalyst's plan for running the query
    demoQuery.explain(true)

    // access underlying rdd.  Since we're dealing with a DataFrame, which is a DataSet[Row]
    // the RDD contains Row objects
    println(demoQuery.rdd.toDebugString)

    //let's use a DataSet
    // DataSets are strongly typed, so we'll need to write a case class for the data contained within
    // See the Person + Name case classes below!

    //We can convert a DataFrame to a DataSet of Person using .as[Person]:
    val ds = df.as[Person]

    //DataFrame looks like its own type for legacy reasons, but under the hood it's just a DataSet[Row]
    // so its nothing too different.  TODO: find the source!

    //We tend to work with DataSets more similarly to working with RDDs, by using higher order functions:
    ds.filter(_.name.first.length() < 6).show()

    // we can chain methods, similar to rdds and earlier collections
    val demoQuery2 = ds
      .filter(_.age > 30)
      .map((person) => { s"${person.name.first} ${person.name.last}" })
      .toDF(
        "Full Name"
      ) //String has a "value" column, so change to DF to provide column name

    demoQuery2.show()

    demoQuery2.explain(true)
    //something to note about our explanation here is the difference in what catalyst knows about DF vs DS
    // in demoQuery2 we only really care about name and age, we filter on age and we produce output
    // based on name.  If we were using a dataframe, catalyst wouldn't even bother reading/processing the other columns
    // Using a DataSet, catalyst doesn't know enough about our data processing to make the optimization
    //The big problem for catalyst here is the map and filter lambdas.  It has no way of knowing which parts
    // of our Person records we're manipulating inside of lambda functions -- it can't look inside our functions.

    //As a bit of a tradeoff, the case classes within DataSets are more efficiently serialized + deserialized
    // than generic Row objects in DataFrames.

    //real quick, using SQL:
    // we can create temp views and write SQL queries to select from them
    //create a dataset:
    val names =
      spark.createDataset(List(Name("Adam", "King"), Name("Jeff", "Goldblum")))

    //create a temp view so I can work with SQL queries:
    names.createOrReplaceTempView("names")

    //use spark.sql to qrite sql queries.  This will return a DataFrame
    spark.sql("SELECT * FROM names").show()

    //So Spark SQL provides 3+1 methods for manipulating and processing your data
    // DataFrames
    // DataSets
    // SQL
    // (and underlying RDDs)
    //We can freely convert between them
    // writing SQL statements returns DataFrames
    // creating temp views transforms DF/DS to SQL tables
    // .as turns a DF to a DS
    // .toDF turns a DS to a DF

    //You should know a bit about each, but you're free to use whatever tool seems most appropriate
    // Keep in mind the DF/DS have some efficiency tradeoffs

  }

  case class Person(
      _id: String,
      index: Long,
      age: Long,
      eyeColor: String,
      phone: String,
      address: String,
      name: Name
  ) {}

  case class Name(first: String, last: String) {}

}
