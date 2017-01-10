package jan_10
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.hadoop.fs._

object SumofOrdersPerADay {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("RetailDB").setMaster("local[*]")

    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)

    val orders = sc.textFile("F:\\Spark&Scala complete\\retail_db\\orders\\part-00000")

    val order_items = sc.textFile("F:\\Spark&Scala complete\\retail_db\\order_items\\part-00000")

    val comletedOrders = orders.filter(line => line.split(",")(3) == "COMPLETE")

    //println(comletedOrders.count())

    val orders2 = comletedOrders.map(x => (x.split(",")(0).toInt, x.split(",")(1)))

    val order_items2 = order_items.map(x => (x.split(",")(1).toInt, x.split(",")(4).toFloat))

    // println(order_items2.count())

    //order_items2.take(10).foreach(println)

    val order_itemsubtotal = order_items2.reduceByKey((x, y) => (x + y))

    //order_itemsubtotal.filter(x => (x._1 == 1)).foreach(println)

    val ordersjoin = orders2.join(order_itemsubtotal)

    //ordersjoin.take(10).foreach(x => println(x._1 + ": \t" + x._2._1 + ":\t" + x._2._2))

    val data = ordersjoin.map(x => (x._2._1, x._2._2))

    val revperday = data.aggregateByKey((0.0, 0))(((acc, value) => (acc._1 + value, acc._2 + 1)),
      (total1, total2) => (total1._1 + total2._1, total1._2 + total2._2))

    //   revperday.take(10).foreach(println)

    // val averagerevperday = revperday.map( x => (x._1, x._2._1/x._2._2))

    val result = revperday.map(x => (x._1, BigDecimal(x._2._1 / x._2._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat))

  //  result.take(10).foreach(println)
    
    val res = result.sortByKey()
    
    res.foreach(println)
    
    res.saveAsTextFile(" .....")


   /* val sorting = result.map(x => (x._2, x._1))
    
    val sorted = sorting.sortByKey( )

    for (s <- sorted) {
      val date = s._2
      val rev = s._1

      println(s"$date \t $rev")

    }*/

  }
}