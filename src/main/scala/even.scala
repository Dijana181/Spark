import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object even {


    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf1 = new SparkConf()
      conf1.setAppName("main")
      conf1.setMaster("local[*]")
      val sc = new SparkContext(conf1)


      val List1 =List(1,2,3,4,5)
      val rdd1=sc.parallelize(List1)
      val rdd2= rdd1.map( x => x*2)
      rdd2.collect()
      val rdd3 = rdd1.filter(x => x %2 ==0)

      var data= rdd2.collect
      data.foreach(println)
    }

  }


