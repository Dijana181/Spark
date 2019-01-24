import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object frame {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf1 = new SparkConf()
      conf1.setAppName("main")
      conf1.setMaster("local[*]")
      val sc = new SparkContext(conf1)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      val Rdd1 = sc.textFile("C:\\Users\\Admin\\Documents\\DijanR\\data.txt")
      val header = Rdd1.first()
      val Rdd2 = Rdd1.filter(row => row != header)
      val Rdd3 = Rdd2.map( x => {
        var record = x.split(",")
        (record(0), record(1).toInt, record(2))
      })

      val df1 = sqlContext.createDataFrame(Rdd3).toDF("Name", "Mark", "Location")
df1.show()

    }


}
