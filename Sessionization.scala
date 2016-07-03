package paytm
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv


//Class to store relevant data from Log Line
case class LogLine(timeStamp: String, ip: String, url: String) 

object Sessionization {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val logFile = "C:/Users/priyankgupta86/Downloads/WeblogChallenge-master/WeblogChallenge-master/data/2015_07_22_mktplace_shop_web_log_sample.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Sessionization Application").setMaster("local")
    //spark context
    val sc = new SparkContext(conf)
    //Hive context to run Hive queries
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    
    val logData = sc.textFile(logFile, 2).cache()
    
    val sessionData = logData.map(_.split(" ")).filter { p => p.length == 26 }.map(p => LogLine(p(0),p(2).toString().split(':')(0), p(12).toString().split('?')(0)))
    
    var sessionDF = sessionData.toDF()
    
    sessionDF.registerTempTable("sessionDF")
    
    //Query to add a new column new_session to separate session boundaries
    var sessionBndry =sqlContext.sql("""
      SELECT *, unix_timestamp(timeStamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'") as curr_timestamp
          , CASE
              WHEN unix_timestamp(timeStamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'")
              -LAG(unix_timestamp(timeStamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'")) 
                 OVER (PARTITION BY ip ORDER BY timeStamp) >= 15 * 60 * 100
              THEN 1
              ELSE 0
          END as new_session 
        FROM
          sessionDF""")
    
    sessionBndry.show()
    sessionBndry.registerTempTable("sessionBndry")
    
    //Query to create new column which will assign numeric values to different sessions
    var sessionIds = sqlContext.sql("""SELECT *
        , SUM(new_session) 
          OVER (PARTITION BY ip ORDER BY timeStamp) AS session_id
        FROM sessionBndry""")
    sessionIds.write.format("com.databricks.spark.csv").save("C:/sessionIds.csv")    
    sessionIds.show()    
    sessionIds.registerTempTable("sessionIds")
    
    
    //Query to get the session time for every session
    var sessionTimes = sqlContext.sql("""SELECT ip, session_id, (max(unix_timestamp(timeStamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'"))
      - min(unix_timestamp(timeStamp, "yyyy-mm-dd'T'hh:mm:ss.SSSSSS'Z'")))/100      
         as session_time from sessionIds group by ip, session_id""")
    sessionTimes.registerTempTable("sessionTimes")
    sessionTimes.show()
    
    
    //Query to get average session time
    var avgSessionTime = sqlContext.sql("""SELECT AVG(session_time) as AvgSessionTime from sessionTimes""")
    avgSessionTime.show()
    
    
    //Query to get unique URL hits for every ip per session
    var uniqueHits = sqlContext.sql("""SELECT ip, session_id, count(distinct url) as uniqueHits from sessionIds group by ip, session_id""")
    uniqueHits.show()
    uniqueHits.write.format("com.databricks.spark.csv").save("C:/uniqueHits.csv")
    
    
    //Query to get the top 100 most engaged users
    var engagedUsers = sqlContext.sql("""SELECT ip, max(session_time) as maxSessionTime from sessionTimes group by 
        ip order by maxSessionTime desc limit 100""")
    engagedUsers.show()
    engagedUsers.write.format("com.databricks.spark.csv").save("C:/engagedUsers.csv")
  }
}