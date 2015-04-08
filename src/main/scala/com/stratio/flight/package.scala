package com.stratio

import org.apache.spark.AccumulableParam
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.stratio.flight.FlightDsl._

/**
 * Created by pmadrigal on 7/04/15.
 */
package object flight {

  var sc = new SparkContext("local", "exampleApp")
  //val errors= sc.accumulable(scala.collection.mutable.Map.empty[String, Int])(AccParam)//sc.accumulable(scala.collection.mutable.Map.empty[String, Int])(Accumulable)

  def main(args: Array[String]) {

    val data2 = sc.textFile("/home/pmadrigal/Escritorio/Spark/Curso Spark/Ejercicio Aeropuerto/1987.csv")
    val PricefuelByYearMonth= sc.textFile("/home/pmadrigal/Escritorio/Spark/Curso Spark/Ejercicio Aeropuerto/Fuel-1987.csv").map(line=> line.split(",")).map(word=> ((word(0), word(1)),word(2).toInt)).collect().toMap//.collect.foreach(println)
    val broadcastFuelPrice= sc.broadcast(PricefuelByYearMonth)

    //println(data2.correctFlight.meanDistanceWithMapReduce)
    val errores= data2.wrongFlight.collect

    println(Flight.errors.value)


   //val distanceByYearMothAirport = csvHealthy.map(flight => ((flight.dates.Year, flight.dates.Month, flight.Origin), flight.Distance.toInt)).reduceByKey(_ + _)
   //val airportDistanceByYearMonth = distanceByYearMothAirport.map(dxairport => ((dxairport._1._1, dxairport._1._2), (dxairport._1._3, dxairport._2)))
    //con join
    //val monthMinPriceByAirport = airportDistanceByYearMonth.join(ymp).map(a=> ((a._2._1._1),(a._1._2, a._2._1._2 * a._2._2))).reduceByKey((a,b)=>if(a._2 >b._2) a else b).collect.foreach(println)

    //Con broadcast
    //val monthMinPriceByAirport = airportDistanceByYearMonth.map(a=> (( a._2._1),(a._1._2, a._2._2.toInt * broadcastFuelPrice.value(a._1) ))).reduceByKey((a,b)=>if(a._2 < b._2) a else b).collect.foreach(println)

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