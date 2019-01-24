import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object test {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf1 = new SparkConf()
      conf1.setAppName("main")
      conf1.setMaster("local[*]")
      val sc = new SparkContext(conf1)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      val Rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\DijanR\\movie.txt")
      val Rdd2 = sc.textFile("C:\\Users\\Admin\\Documents\\DijanR\\movies1.txt")

      val Rdd3 = Rdd1.map( x=>{
        var records = x.split("\t")
        (records(1),records(2))
      })

   val DF1 = sqlContext.createDataFrame(Rdd3)

      DF1.registerTempTable("TestTable")
      sqlContext.sql("SELECT FROM TestTable").show()

    }

}
