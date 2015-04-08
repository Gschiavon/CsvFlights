package com.stratio


import org.apache.spark.SparkContext
import com.stratio.flight.FlightDsl._

/**
 * Created by pmadrigal on 7/04/15.
 */
package object flight {

  var sc = new SparkContext("local", "exampleApp")

  def main(args: Array[String]) {

    val csv = sc.textFile("/home/pmadrigal/Escritorio/Spark/Curso Spark/Ejercicio Aeropuerto/1987.csv")
    val PricefuelByYearMonth= sc.textFile("/home/pmadrigal/Escritorio/Spark/Curso Spark/Ejercicio Aeropuerto/Fuel-1987.csv").map(line=> line.split(",")).map(word=> ((word(0), word(1)),word(2).toInt)).collect().toMap//.collect.foreach(println)
    val broadcastFuelPrice= sc.broadcast(PricefuelByYearMonth)

    val csvHealthy = csv.correctFlight
    csvHealthy.monthMinPriceByAirportWithBroadcast(broadcastFuelPrice).collect.foreach(println)
  }
}

/******************************************************************************************************
  def meanDistance(RDD : RDD[Flight],f: (Flight) ⇒ T,valor : Int): RDD[(String, Double)]= {
       RDD.keyBy(f(_)).groupByKey.mapValues(iterable=>iterable.toList.reduce(_+_).toDouble/iterable.toList.size).collect
      }

  def mean[T](RDD : RDD[Flight],f: (Flight) ⇒ T,valor : Int): RDD[(String, Double)]= {
   RDD.keyBy(f(_)).groupByKey.mapValues(iterable=>iterable.toList.reduce(_+_).toDouble/iterable.toList.size).collect
  }

  ******************************************************************************************************/