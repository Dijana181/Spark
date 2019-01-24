import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object location {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf1 = new SparkConf()
    conf1.setAppName("main")
    conf1.setMaster("local[*]")
    val sc = new SparkContext(conf1)

    val List1 = List(("Dijan","Manchester"),("Peter","Manchester"))

    var Rdd1 = sc.parallelize(List1)
    var Rdd2 = Rdd1.map(x=> s"${x._1} lives in ${x._2} ")
    val data = Rdd2.collect()
    data.foreach(print)
  }

}
