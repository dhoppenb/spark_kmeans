package stackoverflow

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C://winutils")

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)

    val grouped = groupedPostings(raw)

    val scored  = scoredPostings(grouped)//.sample(true, 0.1, 0) //scoredPostings(grouped) //

    val vectors = vectorPostings(scored).partitionBy(new HashPartitioner(langs.length)).cache()
    //assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)

    sc.stop()
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val Qs: RDD[(QID, Posting)] = postings.filter(posting => posting.postingType == 1).keyBy( _.id )
    val As: RDD[(QID, Posting)] = postings.filter(posting => posting.postingType == 2).keyBy( _.parentId.get )
    Qs.join(As).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    //RDD[(QID, Iterable(Q,A))] ==> for each QID, take the Q out of the first elem of Iterable and
    //                              get the answerHighScore out of a slice of all A's of Iterable
    grouped.map{ case (_,iter) => (iter.head._1, answerHighScore(iter.map(_._2).toArray)) }
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    scored.map{ case (q,score) => (firstLangInTag(q.tags,langs).get * langSpread,score) }
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }

  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans = means.clone() // you need to compute newMeans

    //: RDD[(Int,(LangIndex, HighScore))]
    val closest = vectors.map(p => (findClosest(p, means), p))
    //: RDD[(Int,Iterable[(LangIndex, HighScore)])]
    val closestGrouped = closest.groupByKey()

    //update the newMeans items based on their associated vector points

    val updatedMeans = closestGrouped.mapValues(averageVectors).collect()
    //put back into newMeans to ensure we end up with the same number of of means...
    updatedMeans.foreach({ case (index, point) => newMeans.update(index, point)})

    //Check how much the means moved this time
    println( s"means ${means.toSeq}" )
    println( s"newms ${newMeans.toSeq}" )

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }

  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def shortList():Seq[(Int,Int)] = {
    var pointlist = List((0,0))

    val rand = scala.util.Random
    val numpoints = 40
    val numclusters = 4
    val spread = 10
    var mean = (1,1)

    for(i <- 0 to numpoints){
      if(i % (numpoints / numclusters) == 0)
        mean = (rand.nextInt(100), rand.nextInt(100))
      pointlist = (mean._1 - (spread/2)+rand.nextInt(spread),
        mean._2 - (spread/2)+rand.nextInt(spread)) +: pointlist
    }
    pointlist
  }


  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    //: RDD[(Int,(LangIndex, HighScore))] //where Int is index in means
    val closest = vectors.map(p => (findClosest(p, means), p))
    //: RDD[(Int,Iterable[(LangIndex, HighScore)])]
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>

      val groupbylang = vs.map{
        //calc normal index again and group by langnumber
        case (langindex, highscore) => (langindex/langSpread, highscore)
      }.groupBy(_._1)
      //out type: seq[LangNr, Iterable[(LangNr, HighScore)]]

      val mostFreqLangIndex = groupbylang.maxBy(_._2.size)._1

      val langLabel: String     = langs( mostFreqLangIndex )
      // percent of the questions in the most common language
      val langPercent: Double   = 100.0 * groupbylang(mostFreqLangIndex).size / vs.size
      val clusterSize: Int      = vs.size
      val sortedScores: Seq[Int]= vs.map(_._2).toList.sorted
      val mid                   = clusterSize/2
      val medianScore: Int      = if(clusterSize == 0 ) -1 else {
        if(clusterSize % 2 == 0) (sortedScores(mid-1) + sortedScores(mid) / 2)
        else sortedScores(mid)
      }


      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
