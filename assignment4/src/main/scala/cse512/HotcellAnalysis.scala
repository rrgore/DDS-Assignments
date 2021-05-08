package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.ArrayBuffer

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val numEntries = pickupInfo.count()

  // Group by x, y and z
  var groupedPickupInfo = pickupInfo.withColumn("occur", lit("1").cast(IntegerType))
  groupedPickupInfo.createOrReplaceTempView("grppickupinfo")
  groupedPickupInfo = spark.sql("select x, y, z, count(occur) from grppickupinfo group by x, y, z")
  newCoordinateName = Seq("x", "y", "z", "occur")
  groupedPickupInfo = groupedPickupInfo.toDF(newCoordinateName:_*)
  groupedPickupInfo.createOrReplaceTempView("grppickupinfo")

  var sumSqOccurs = 0.0
  val groupedPickupInfoRows = groupedPickupInfo.collect()
  for ( row <- groupedPickupInfoRows ) {
    val currOcc = row.getLong(3)
    sumSqOccurs += (currOcc * currOcc)
  }

  HotcellUtils.initUtil(minX, minY, minZ, maxX, maxY, maxZ, numCells, numEntries, sumSqOccurs)

  var adjCombinedPickupDF = spark.sql("select g1.x as x, g1.y as y, g1.z as z, g1.occur, g2.occur as adjoccur"+
                                " from grppickupinfo g1, grppickupinfo g2"+
                                " where g1.x in (g2.x-1, g2.x, g2.x+1)"+
                                " and g1.y in (g2.y-1, g2.y, g2.y+1)"+
                                " and g1.z in (g2.z-1, g2.z, g2.z+1)")
  adjCombinedPickupDF.createOrReplaceTempView("adjcombinedpickup")
  adjCombinedPickupDF = spark.sql("select x, y, z, sum(adjoccur) as sumadjoccur, count(adjoccur) as cntadjoccur"+
                              " from adjcombinedpickup group by x, y, z")
  adjCombinedPickupDF.createOrReplaceTempView("adjcombinedpickup")

  // For each cell, perform Gi
  spark.udf.register("CalculateGi",(sumadjoccur: Long, cntadjoccur: Long)=>(
    HotcellUtils.CalculateGscore(sumadjoccur, cntadjoccur)
    ))
  var hotcellDF = spark.sql("select x, y, z, CalculateGi(sumadjoccur, cntadjoccur) as gscore"+
                              " from adjcombinedpickup")
  hotcellDF.createOrReplaceTempView("hotcell")
  hotcellDF = spark.sql("select x, y, z from hotcell order by gscore desc")
  hotcellDF.createOrReplaceTempView("hotcell")

  return hotcellDF; // YOU NEED TO CHANGE THIS PART
}
}
