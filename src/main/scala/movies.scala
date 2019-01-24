import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object movies {


    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf1 = new SparkConf()
      conf1.setAppName("main")
      conf1.setMaster("local[*]")
      val sc = new SparkContext(conf1)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      val Rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\DijanR\\movie.txt")

      val Rdd2 = Rdd1.map(x => {
        var record = x.split("\t")
        (record(0), record(1), record(2), record(3))
      })

     val Rdd3 = Rdd2.filter(rate => rate._3 =="5")
     val DF1 = sqlContext.createDataFrame(Rdd3)
     val DF2 = DF1.groupBy("_2").count()
     val DF3 = DF2.orderBy(desc("count"))

     val Rdd4 = sc.textFile("C:\\Users\\Admin\\Documents\\DijanR\\movies1.txt")
     val Rdd5 = Rdd4.map(x => {
        var records = x.split("\\|")
        (records(0), records(1))
      })

      val DF4 = sqlContext.createDataFrame(Rdd5)

      val Rf = DF3.join(DF4,DF3("_2")===DF4("_1"), "left_outer")
      val Rf1 = Rf.orderBy(desc("count"))

      Rf1.show()

    }

}
