import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object lists {



    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf1 = new SparkConf()
      conf1.setAppName("main")
      conf1.setMaster("local[*]")
      val sc = new SparkContext(conf1)

      val List1 = List("Dec", "January", "February", "March", "April", "May")
      val List2 = List("May", "June","July", "August", "Sept", "Oct", "Nov", "Dec")

      val rdd1 = sc.parallelize(List1)
      val rdd2 = sc.parallelize(List2)

      val rdd3 = rdd1.union(rdd2)
      val data1 = rdd3.collect()
      data1.foreach(print)

      val rdd4 = rdd1.intersection(rdd2)
      val data2 = rdd4.collect()
      data2.foreach(print)

      val rdd5 = rdd1.subtract(rdd2)
      val data3 = rdd5.collect()
      data3.foreach(print)



    }



}
