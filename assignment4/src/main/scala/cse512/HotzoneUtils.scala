package cse512

import scala.collection.mutable.ArrayBuffer

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    // Assumption: The rectangle is parallel to the grid
    // Create list of x1,y1,x2,y2
    val rectCoords = getCoords(queryRectangle)

    // Create list x3,y3
    val pointCoords = getCoords(pointString)

    // If x1<=x3<=x2 and y1<=y3<=y2, return true else return false
    return comparePointToRect(rectCoords, pointCoords)
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS


  def getCoords(queryRectangle: String): ArrayBuffer[Double] = {
    var coords = ArrayBuffer[Double]()
    val rectStrings = queryRectangle.split(",")
    for ( coord <- rectStrings ) {
      coords += coord.toDouble
    }

    return coords
  }

  // On-boundary returns true
  def comparePointToRect(rect: ArrayBuffer[Double], point: ArrayBuffer[Double]): Boolean = {

    return compareCoord(rect(0), rect(2), point(0)) && compareCoord(rect(1), rect(3), point(1))
  }

  def compareCoord(rCoord1: Double, rCoord2: Double, pCoord: Double): Boolean = {
    if (rCoord1 < rCoord2 && pCoord >= rCoord1 && pCoord <= rCoord2) {
      return true
    } else if(rCoord1 > rCoord2 && pCoord <= rCoord1 && pCoord >= rCoord2) {
      return true
    }
    return false
  }
}
