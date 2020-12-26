import org.apache.commons.math3.util.FastMath.{pow, sqrt}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(cfg)
    //val textFile = sc.textFile("file:///C:/Users/YNLKTK/Documents/список продуктов.txt")
    //textFile.foreach(println)
    //sc.stop()

    val tripData = sc.textFile("file:///C:/Users/YNLKTK/Documents/big_data/data/trips.csv")
    // запомним заголовок, чтобы затем его исключить
    val tripsHeader = tripData.first
    val trips = tripData.filter(row=>row!=tripsHeader).map(row=>row.split(",",-1))
    val stationData = sc.textFile("file:///C:/Users/YNLKTK/Documents/big_data/data/stations.csv")
    val stationsHeader = stationData.first
    val stations = stationData.filter(row=>row!=stationsHeader).map(row=>row.split(",",-1))
    //println("Headers")
    //println(tripsHeader)
    //println(stationsHeader)

    //println("Some data from trips")
    //trips.take(5).foreach(println)
    //println("Some data from stations")
    //stations.take(5).foreach(println)

    val stationsIndexed = stations.keyBy(row=>row(0).toInt)
    stationsIndexed.collect()
    //stationsIndexed.take(5).foreach(println)
    val tripsByStartTerminals = trips.keyBy(row=>row(4).toInt)
    val tripsByEndTerminals = trips.keyBy(row=>row(7).toInt)

    val startTrips = stationsIndexed.join(tripsByStartTerminals)
    val endTrips = stationsIndexed.join(tripsByEndTerminals)
/*
    println(startTrips.toDebugString)
    println(endTrips.toDebugString)

    println(startTrips.count())
    println(endTrips.count())

    stationsIndexed.partitionBy(new
        HashPartitioner(trips.partitions.size))
    println(stationsIndexed.partitioner)
*/
    case class Station(
                        stationId:Integer,
                        name:String,
                        lat:Double,
                        long:Double,
                        dockcount:Integer,
                        landmark:String,
                        installation:String,
                        notes:String)
    case class Trip(
                     tripId:Integer,
                     duration:Integer,
                     startDate:LocalDateTime,
                     startStation:String,
                     startTerminal:Integer,
                     endDate:LocalDateTime,
                     endStation:String,
                     endTerminal:Integer,
                     bikeId: Integer,
                     subscriptionType: String,
                     zipCode: String)

    val tripsInternal = trips.mapPartitions(rows => {
      val timeFormat =
        DateTimeFormatter.ofPattern("yyyy-MM-dd H:m")
      rows.map( row =>
        new Trip(tripId=row(0).toInt,
          duration=row(1).toInt,
          startDate= LocalDateTime.parse(row(2), timeFormat),
          startStation=row(3),
          startTerminal=row(4).toInt,
          endDate=LocalDateTime.parse(row(5), timeFormat),
          endStation=row(6),
          endTerminal=row(7).toInt,
          bikeId=row(8).toInt,
          subscriptionType=row(9),
          zipCode=row(10)))
    })

    //println(tripsInternal.first)
    //println(tripsInternal.first.startDate)

    val stationsInternal = stations.map(row=>
      new Station(stationId=row(0).toInt,
        name=row(1),
        lat=row(2).toDouble,
        long=row(3).toDouble,
        dockcount=row(4).toInt,
        landmark=row(5),
        installation=row(6),
        notes=null))

    val tripsByStartStation = tripsInternal.keyBy(record =>
      record.startStation)
    val avgDurationByStartStation = tripsByStartStation
      .mapValues(x=>x.duration)
      .groupByKey()
      .mapValues(col=>col.reduce((a,b)=>a+b)/col.size)

    //avgDurationByStartStation.take(10).foreach(println)
    //println("---------------")

    val avgDurationByStartStation2 = tripsByStartStation
      .mapValues(x=>x.duration)
      .aggregateByKey((0,0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2))
      .mapValues(acc=>acc._1/acc._2)

    //avgDurationByStartStation2.take(10).foreach(println)

    val firstGrouped = tripsByStartStation
      .reduceByKey((trip1,trip2) =>
    if (trip1.startDate.compareTo(trip2.startDate)<0)
      trip1 else trip2)
    //firstGrouped.take(10).foreach(println)


    //1. Найти велосипед с максимальным пробегом.
    val bikes = tripsInternal.keyBy(record => record.bikeId)
      .mapValues(x => x.duration)
      .groupByKey().mapValues(col=>col.reduce((a,b)=>a+b)).max()
    println("велосипед с максимальным пробегом:")
    println(bikes._1)

    //2. Найти наибольшее расстояние между станциями.
    val dataOfStations = stationsInternal.cartesian(stationsInternal)
      .map {
        case (station1, station2) =>
          (station1.long, station1.lat, station1.stationId,
            station2.long, station2.lat, station2.stationId)
      }
    val maxStationDistance = dataOfStations.map{ row => (sqrt(pow(row._1 - row._4,2) + pow(row._2 - row._5,2)), row._3 ,  row._6 ) }
      .sortBy( a => a._1,ascending = false)
    println("наибольшее расстояние между станциями:")
    maxStationDistance.collect().take(1).foreach(println)

    //3. Найти путь велосипеда с максимальным пробегом через станции.
    val bikeMaxRun = tripsInternal.keyBy(record => record.bikeId).lookup(bikes._1).map(x => x.startStation)
    println("путь велосипеда с максимальным пробегом через станции:")
    println(bikeMaxRun)

    //4. Найти количество велосипедов в системе.
    val bikesNumber = tripsInternal.keyBy(record => record.bikeId)
      .mapValues(x => x.bikeId).groupByKey().distinct().count()
    println("количество велосипедов в системе:")
    println(bikesNumber)

    //5. Найти пользователей, потративших на поездки более 3 часов.
    val userMoreThreeHours = tripsInternal.keyBy(record => record.zipCode)
      .mapValues((x) => x.duration).filter(t => t._2 > 180)
    println("пользователи, потратившие на поездки более 3 часов:")
    userMoreThreeHours.take(10).foreach(println)

    sc.stop()
  }
}
