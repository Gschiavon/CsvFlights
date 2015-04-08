package com.stratio.flight

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * Created by pmadrigal on 7/04/15.
 */
class ErrorsOrHealthyFlights(rdd: RDD[String]){

  private def toParsedFlight: RDD[Either[Seq[(String,String)], String]] ={
    val header =  rdd.first()
    rdd.filter(!_.contains(header)).map(linea=> Flight.analizarLista(linea, linea.split(",")))
  }

  def wrongFlight: RDD[(String, String)]= toParsedFlight.filter(_.isLeft).flatMap(_.left.get)

  def correctFlight: RDD[Flight]= toParsedFlight.filter(_.isRight).map(_.right.get).map(lineCorrect => Flight.apply(lineCorrect.split(",")))
}
class FlightOperation(rdd: RDD[Flight])
{
  def meanDistanceWithAggregate: Double= {
    val result1 = rdd.map(flight=> flight.Distance.toInt).aggregate((0,0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    result1._1.toDouble/result1._2
  }

  def meanDistance: Double = rdd.map(flight=> flight.Distance.toInt).mean()

  def meanDistanceWithMapReduce: Double = {
    val result3 = rdd.map(flight => (flight.Distance.toInt, 1)).reduce((dato1, dato2) => (dato1._1 + dato2._1, dato1._2 + dato2._2))
    result3._1.toDouble / result3._2
  }

  def meanDistanceWithGroupByKey: Double = {
    val result4= rdd.map(flight => (1, flight.Distance.toInt)).groupByKey.mapValues(Distance => Distance.toList.reduce(_ + _).toDouble / Distance.toList.size)//.map(mediaByInt=> mediaByInt._2)
    result4.reduce((a,b)=> b)._2
  }

  def monthMinPriceByAirportWithBroadcast(implicit broadcastFuelPrice: Broadcast[Map[(String, String), Int]]): RDD[(String, (String, Int))]= {

    val distanceByYearMothAirport = rdd.map(flight => ((flight.dates.Year, flight.dates.Month, flight.Origin), flight.Distance.toInt)).reduceByKey(_ + _)
    val airportDistanceByYearMonth = distanceByYearMothAirport.map(dxairport => ((dxairport._1._1, dxairport._1._2), (dxairport._1._3, dxairport._2)))

    airportDistanceByYearMonth.map(a => ((a._2._1), (a._1._2, a._2._2.toInt * broadcastFuelPrice.value(a._1)))).reduceByKey((a, b) => if (a._2 < b._2) a else b)
  }

}

trait FlightDsl {
  implicit def errorsOrHealthyFlights(rdd: RDD[String]): ErrorsOrHealthyFlights = new ErrorsOrHealthyFlights(rdd)
  implicit def FlightOperation(rdd: RDD[Flight]): FlightOperation = new FlightOperation(rdd)
}

object FlightDsl extends FlightDsl
