package com.stratio.flight

import org.apache.spark.AccumulableParam

/**
 * Created by pmadrigal on 30/03/15.
 */

//Year: String, Month: String,	DayofMonth: String,	DayOfWeek: String,	DepTime: String,	CRSDepTime: String, ArrTime: String,	CRSArrTime: String,	UniqueCarrier: String, FlightNum: String, TailNum: String,	ActualElapsedTime: String,	CRSElapsedTime: String,	AirTime: String,	ArrDelay: String,	DepDelay: String,	Origin: String,	Dest: String,	Distance: String,	TaxiIn: String,	TaxiOut: String,	Cancelled: String,	CancellationCode: String,	Diverted: String,	CarrierDelay: String,	WeatherDelay: String,	NASDelay: String,	SecurityDelay: String,	LateAircraftDelay: String

case class Flight (dates: Date, times: Time, delays: Delay, UniqueCarrier: String, FlightNum: String, TailNum: String,	Origin: String,	Dest: String,	Distance: String,	TaxiIn: String,	TaxiOut: String,	Cancelled: String,	CancellationCode: String,	Diverted: String)
{}

object Flight{

  val errors = sc.accumulable(scala.collection.mutable.Map.empty[String, Int])(AccParam)

  def apply(fields: Array[String]): Flight = {
    val (firstChuck, secondChuck) = fields.splitAt(22)

    val Array(year, month,	dayofMonth,	dayOfWeek, departure, crsDeparture, arrive, crsArrive,
    uniqueCarrier, number, tailNumber, actualElapsed, crsElapsed, airTime, arriveDelay,
    departureDelay, origin, destination, distance, taxiIn, taxiOut, cancelled) = firstChuck

    val Array(cacellationCode, diverted, carrierDelay, weatherDelay, nasDelay, securityDelay,
    lateAircraftDelay) = secondChuck

    val date= Date(year, month,	dayofMonth,	dayOfWeek)
    val time= Time(departure ,crsDeparture ,arrive ,crsArrive ,actualElapsed ,crsElapsed ,airTime )
    val delay= Delay(arriveDelay ,departureDelay ,carrierDelay, weatherDelay, nasDelay, securityDelay, lateAircraftDelay)
    Flight(date, time, delay, uniqueCarrier, number, tailNumber, origin, destination, distance, taxiIn, taxiOut, cancelled , cacellationCode, diverted)
  }

  def analizarLista(linea : String, camposLinea : Seq[String]): Either[Seq[(String,String)], String]= {

    val columnasInt = Seq(0,1,2,3,4,5,6,7,9,11,12,14,15,18,21,23)
    val columnasString = Seq(8,16,17)
    val columnasNa = Seq(10,13,19,20,22,24,25,26,27,28)

    val enteros=  columnasInt.flatMap(columna => tryInt(linea,camposLinea(columna)))
    val cadenas=  columnasString.flatMap(columna => tryString(linea,camposLinea(columna)))
    val na=  columnasNa.flatMap(columna =>tryNa(linea,camposLinea(columna)))

    val salida=(enteros ++ cadenas ++ na)

    if(!(enteros ++ cadenas ++ na ).isEmpty)
      Left(salida)
    else
      Right(linea)
  }
  private def tryNa (linea : String,cadena: String): Option[(String,String)]={

    if (cadena.compareTo( "NA")==0)
      None
    else
      Some(("ErrorTipo3",linea))
  }

  private def tryString (linea : String,cadena: String): Option[(String,String)]={

    if(cadena.compareTo("NA")==0)
    {
      errors.add("ErrorTipo1")
      Some(("ErrorTipo1",linea))
    }
    else
      None
  }

  private def tryInt(linea : String, cadena: String): Option[(String,String)]={
    try{
      if(cadena.compareTo("NA")==0)
      { errors.add("ErrorTipo1")
        Some(("ErrorTipo1",linea))
      }
      else
      {
        cadena.toInt
        None
      }
    } catch {
      case e: Exception =>
        errors.add("ErrorTipo2")
        Some(("ErrorTipo2",linea))
    }
  }

  object AccParam extends AccumulableParam[scala.collection.mutable.Map[String, Int], String] {

    def zero(initialValue: scala.collection.mutable.Map[String, Int]) = scala.collection.mutable.Map.empty[String, Int]

    def addInPlace(r1: scala.collection.mutable.Map[String, Int], r2: scala.collection.mutable.Map[String, Int]): scala.collection.mutable.Map[String, Int] = {
      r2.foreach{ kv => add(r1, kv._1, kv._2) }
      r1
    }

    def addAccumulator(result: scala.collection.mutable.Map[String, Int], errorType: String): scala.collection.mutable.Map[String, Int] = {
      add(result, errorType, 1)
      result
    }

    private def add(result: scala.collection.mutable.Map[String, Int], key: String, value: Int): Unit = {
      result.put(key, result.getOrElse(key, 0) + value)
    }
  }
}



case class Date(Year: String, Month: String,	DayofMonth: String,	DayOfWeek: String)
{}

case class Time(DepTime: String,	CRSDepTime: String, ArrTime: String,	CRSArrTime: String, ActualElapsedTime: String,	CRSElapsedTime: String,	AirTime: String)
{}

case class Delay(ArrDelay: String,	DepDelay: String,CarrierDelay: String,	WeatherDelay: String,	NASDelay: String,	SecurityDelay: String,	LateAircraftDelay: String)
{}
