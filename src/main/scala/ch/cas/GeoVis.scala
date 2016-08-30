package ch.cas

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import scala.collection.mutable._

import ch.ninecode._
import ch.ninecode.cim._
import ch.ninecode.model._


class GeoVis extends Serializable
{
  
  case class SpezificAcLineSegment(id: String, name: String, aliasName: String, location: String)
  case class BBox(xmin: Double, ymin: Double, xmax: Double, ymax: Double)
  
  
  def extract (sc: SparkContext, sqlContext: SQLContext, args: String): DataFrame =
  {
    val arguments = args.split (",").map (
            (s) =>
                {
                    val pair = s.split ("=")
                    if (2 == pair.length)
                        (pair(0), pair(1))
                    else
                        (pair(0), "")
                }
        ).toMap
            
    val xmin = arguments.get("xmin").get.toDouble
    val ymin = arguments.get("ymin").get.toDouble
    val xmax = arguments.get("xmax").get.toDouble
    val ymax = arguments.get("ymax").get.toDouble
    val maxLines = arguments.get("maxLines").get.toInt   
    
    val bbox = BBox (xmin, ymin, xmax, ymax)
    
    val positionPoint = get (sc, "PositionPoint").asInstanceOf[RDD[PositionPoint]]
    val acLineSegment = get (sc, "ACLineSegment").asInstanceOf[RDD[ACLineSegment]]
    
    val line = acLineSegment.map((line: ACLineSegment) => { SpezificAcLineSegment (line.sup.sup.sup.sup.sup.mRID, line.sup.sup.sup.sup.sup.name, line.sup.sup.sup.sup.sup.aliasName, line.sup.sup.sup.sup.Location) })
    val filteredOrderedPositions = preparePositionPoints(positionPoint, bbox)
 
    val lines = line.keyBy(_.location).join(filteredOrderedPositions)
    
    val ppDf = sqlContext.createDataFrame (lines.values)
    return ppDf
  }
  
    
  def preparePositionPoints(pp: RDD[PositionPoint], bbox: BBox): RDD[(String, List[String])] =
  {
    
    val filteredPoints = pp.filter((pp: PositionPoint) => 
    {
      var xPos = pp.xPosition
      var yPos = pp.yPosition
      (xPos.toDouble >= bbox.xmin && 
       yPos.toDouble >= bbox.ymin && 
       xPos.toDouble <= bbox.xmax && 
       yPos.toDouble <= bbox.ymax)
    })
      
    val groupedPoints = filteredPoints.map((pp: PositionPoint) => (pp.Location, (pp.sequenceNumber, pp.xPosition, pp.yPosition)))
                                      .groupByKey()
       
    val flattenPoints = groupedPoints.mapValues(value => 
      {
        var orderedPoints = List[Seq[String]]()
        val it = value.iterator
        while (it.hasNext) 
        {
          var point = it.next()
          orderedPoints = orderedPoints :+ Seq(point._2, point._3)
        }
        val flatten = orderedPoints.flatten
        flatten
      })   
      
    return flattenPoints
  }
  
  
  def get (sc: SparkContext, name: String): RDD[Element] =
  {
    val rdds = sc.getPersistentRDDs
    for (key <- rdds.keys)
    {
        val rdd = rdds (key)
        if (rdd.name == name)
            return (rdd.asInstanceOf[RDD[Element]])
    }
    return (null)
  }
  

  def generalize() {
    
    
    
  }
}
