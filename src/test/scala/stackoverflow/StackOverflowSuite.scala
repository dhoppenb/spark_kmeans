package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("two Q's with 2 and 1 A's groupedPosting test"){
    import StackOverflow._
    val rdd = sc.parallelize(
      Seq( Posting(1,1,None,None,1,None),
        Posting(2,2,None,Some(1),1,None),
        Posting(2,3,None,Some(1),1,None),
        Posting(1,10,None,None,1,None),
        Posting(2,11,None,Some(10),1,None)
      ))
    val res = groupedPostings(rdd)
    assert(res.collect().toList == List((1,Iterable((Posting(1,1,None,None,1,None),Posting(2,2,None,Some(1),1,None)), (Posting(1,1,None,None,1,None),Posting(2,3,None,Some(1),1,None)))), (10,Iterable((Posting(1,10,None,None,1,None),Posting(2,11,None,Some(10),1,None)))))
    , "failed to create proper grouping")
    sc.stop()
  }

}
/*
case class Posting(
postingType: Int,
id: Int,
acceptedAnswer: Option[Int],
parentId: Option[QID],
score: Int,
tags: Option[String]) extends Serializable
 */