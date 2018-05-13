import java.net.URLEncoder

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
//import spark.implicits._
import org.apache.spark.sql.Row
//import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Github {
  val usersPerURL = 120
  var technology = ""
  var language = ""
  def main(args: Array[String]) {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    language = args(0)
    technology = args(1)

    val spark: SparkSession = SparkSession.builder.master("local").config("spark.cores.max", "8").getOrCreate // Remove master when deploying.
    //    val spark: SparkSession = SparkSession.builder.getOrCreate
    val sc = spark.sparkContext // Just used to create test RDDs
    // Create the target url string for git API for Socrata data source
    val gituri = "https://api.github.com/search/repositories?q=" + URLEncoder.encode(technology, "UTF-8") + "+language:" + URLEncoder.encode(language, "UTF-8") + "&sort=stars&order=desc"
    ///Say we need to call the API for 3 sets of input parameters for different values of 'region' and 'source'. The 'region' and 'source' are two filters supported by the git API for Socrata data source

    var a: List[(String, String)] = List()

    val per_page = "12"
    var count = 1
    while (count <= 10) {
      a = a :+ ((count.toString(), per_page))
      count += 1

    }
    // Now we create a RDD using these input parameter values

    val gitinputRdd = sc.parallelize(a)

    // Next we need to create the DataFrame specifying specific column names that match the field names we wish to filter on
    val gitinputKey1 = "page"
    val gitinputKey2 = "per_page"

    val gitDf = spark.createDataFrame(gitinputRdd).toDF(gitinputKey1, gitinputKey2)

    // And we create a temporary table now using the gitDf
    gitDf.createOrReplaceTempView("gitinputtbl")

    // Now we create the parameter map to pass to the REST Data Source.

    val parmg = Map("url" -> gituri, "input" -> "gitinputtbl", "method" -> "GET", "userId" -> "matvaibhav1", "userPassword" -> "******", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "1", "schemaSamplePcnt" -> "5", "callStrictlyOnce" -> "Y")
    printf(gituri)
    // Now we create the Dataframe which contains the result from the call to the git API for the 3 different input data points
    val gitsDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

    gitsDf.createOrReplaceTempView("gitstbl")

    //    spark.sql("SELECT COUNT(*) from gitstbl")
    gitsDf.printSchema() //
    val dfContent = gitsDf.select(explode(gitsDf("output.items"))).toDF("content")

    // dfContent.printSchema
    val owners: List[Row] = dfContent.select("content.owner.login").collectAsList().asScala.toList
    //    println(owners.size.toString())
    val uris = constructOwnerUrls(owners)
    val itUris = uris.iterator
    var countOwners = 0
    while (itUris.hasNext) {
      val uri = itUris.next()
      println(uri)
      countStars(uri, owners.slice(count, count + 50), spark, sc)
      countOwners += 50
    }

    spark.stop;
  }
  def countStars(uri: String, owners: List[Row], spark: SparkSession, sc: SparkContext) = {
    var a: List[(String, String)] = List()
    //Make a anonymous call to get num of rows
    //getCount(uri, spark, sc)
    val per_page = "100" //TODO keep 100
    var count = 1
    while (count <= 10) {
      a = a :+ ((count.toString(), per_page))
      count += 1

    }
    val gitinput1 = ("1", "50")
    val gitinput2 = ("2", "50")
    // Now we create a RDD using these input parameter values

    val gitinputRdd = sc.parallelize(a) //TODO works only if the page has >0 rows
    //    val gitinputRdd = sc.parallelize(List(gitinput1)) //TODO works only if the page has >0 rows

    // Next we need to create the DataFrame specifying specific column names that match the field names we wish to filter on
    val gitinputKey1 = "page"
    val gitinputKey2 = "per_page"

    val gitDf = spark.createDataFrame(gitinputRdd).toDF(gitinputKey1, gitinputKey2)

    // And we create a temporary table now using the gitDf
    gitDf.createOrReplaceTempView("gitinputtbl")

    // Now we create the parameter map to pass to the REST Data Source.

    val parmg = Map("url" -> uri, "input" -> "gitinputtbl", "method" -> "GET", "userId" -> "matvaibhav", "userPassword" -> "*******", "readTimeout" -> "100000", "connectionTimeout" -> "2000", "partitions" -> "1", "schemaSamplePcnt" -> "5", "callStrictlyOnce" -> "Y")

    // Now we create the Dataframe which contains the result from the call to the git API for the 3 different input data points
    val gitsDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

    val dfContent = gitsDf.select(explode(gitsDf("output.items"))).toDF("content")

    if (dfContent.count() > 0) { //TODO do with table

      dfContent.createOrReplaceTempView("table")
      val dfStarSum = spark.sql("SELECT content.owner.login, content.owner.html_url, SUM(content.stargazers_count) FROM table  GROUP BY content.owner.login, content.owner.html_url LIMIT 100").toDF("User", "Github URL", "Star Sum")
      //      dfStarSum.so
      dfStarSum.sort(org.apache.spark.sql.functions.col("Star Sum").desc).show(200)
      dfStarSum.sort(org.apache.spark.sql.functions.col("Star Sum").desc).coalesce(1).write.option("header", "true").mode("append").csv("output")

    }
  }
  def constructOwnerUrls(owners: List[Row]): ListBuffer[String] = {
    var uris = new ListBuffer[String]
    var uri: StringBuilder = new StringBuilder
    uri.append("https://api.github.com/search/repositories?q=")
    if (technology != null && !technology.isEmpty()) {
      uri.append(URLEncoder.encode(technology, "UTF-8") + "+")
    }
    val it = owners.iterator
    var count: Int = 0
    while (it.hasNext && uri.length <= 2000) {
      val owner = it.next().get(0).toString()
      uri.append("user:" + owner + "+")
      count += 1;
      if (count == usersPerURL) {
        //        println(uri)
        uris += uri.toString()
        //        println(uris)
        //        println(uris.size)
        uri.setLength(0)
        uri.append("https://api.github.com/search/repositories?q=")
        if (technology != null && !technology.isEmpty()) {
          uri.append(URLEncoder.encode(technology, "UTF-8") + "+")
        }
        count = 0
      }
    }
    if (count > 0)
      uris += uri.toString()

    uri.setLength(0)
    if (language != null && !language.isEmpty()) {

      uri.append("language:" + URLEncoder.encode(language, "UTF-8"))
    }
    if (uri.lastIndexOf("+") + 1 == uri.length) { //test
      //remove last "+"
      uri.replace(uri.lastIndexOf("+"), uri.lastIndexOf("+") + 1, "")
    }
    uri.append("&sort=stars&order=desc")
    var i = 0
    while (i < uris.length) {
      uris.update(i, uris(i) + uri)
      i += 1
    }
    return uris
  }

}