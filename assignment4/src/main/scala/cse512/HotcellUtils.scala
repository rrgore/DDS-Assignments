package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import scala.math.sqrt

object HotcellUtils {
  val coordinateStep = 0.01
  val sumWeights = 26
  var minX = 0.0
  var maxX = 0.0
  var minY = 0.0
  var maxY = 0.0
  var minZ = 0.0
  var maxZ = 0.0
  var avgEntries = 0.0
  var numCells = 0.0
  var numEntries = 0.0
  var sumSqEntries = 0.0

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def CalculateGscore (sumAdjOccurs: Long, cntAdjOccurs: Long): Double =
  {
    // Getis Ord equation for finding Gi is as follows
    //
    //              Sum( wi*xi ) - adjC*X
    // Gi = ---------------------------------------
    //       +------------------  +---------------
    //       | Sum( x^2 )         | adj*n - adj^2
    //       |----------- - X^2   |-------------
    //      _|     n             _|    n - 1
    //

    val numTerm1 = sumAdjOccurs
    this.avgEntries = numEntries/numCells
    val numTerm2 = cntAdjOccurs * avgEntries
    val numTerm = numTerm1 - numTerm2
    val denTerm1 = sqrt((sumSqEntries/numCells) - (avgEntries*avgEntries))
    val denTerm2 = sqrt((cntAdjOccurs*numCells - (cntAdjOccurs*cntAdjOccurs)) / (numCells - 1))
    val denTerm = denTerm1 * denTerm2
    val Gi = numTerm/denTerm
    Gi
  }

  def initUtil (minX: Double, maxX: Double, minY: Double, maxY: Double,
                minZ: Double, maxZ: Double, numCells: Double, numEntries: Long,
                sumSqEntries: Double): Unit =
  {
    this.minX = minX
    this.maxX = maxX
    this.minY = minY
    this.maxY = maxY
    this.minZ = minZ
    this.maxZ = maxZ
    this.numCells = numCells
    this.numEntries = numEntries
    this.sumSqEntries = sumSqEntries
  }
}
