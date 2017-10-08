package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

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

  test("groupedPosting groups answers and questions together") {
    val postings = StackOverflow.sc.parallelize(List(
      Posting(1,27233496,None,None,0,Some("C#")),
    Posting(1,23698767,None,None,9,Some("C#")),
    Posting(1,5484340,None,None,0,Some("C#")),
    Posting(2,5494879,None,Some(5484340),1,None),
    Posting(1,9419744,None,None,2,Some("Objective-C")),
    Posting(1,26875732,None,None,1,Some("C#")),
    Posting(1,9002525,None,None,2,Some("C++")),
    Posting(2,9003401,None,Some(9002525),4,None),
    Posting(2,9003942,None,Some(9002525),1,None),
    Posting(2,9005311,None,Some(9002525),0,None)
    ))
    val groupedPosts = testObject.groupedPostings(postings).collect().toList
    assert(groupedPosts ==
      List(
        (5484340, List((Posting(1,5484340,None,None,0,Some("C#")),Posting(2,5494879,None,Some(5484340),1,None)))),
        (9002525, List((Posting(1,9002525,None,None,2,Some("C++")),Posting(2,9003401,None,Some(9002525),4,None)),
          (Posting(1,9002525,None,None,2,Some("C++")),Posting(2,9003942,None,Some(9002525),1,None)),
          (Posting(1,9002525,None,None,2,Some("C++")),Posting(2,9005311,None,Some(9002525),0,None))))
      )
    )
  }

  test("scoredPostings gives the highest score for every question") {
    val postings = StackOverflow.sc.parallelize(List(
      Posting(1,27233496,None,None,0,Some("C#")),
      Posting(1,23698767,None,None,9,Some("C#")),
      Posting(1,5484340,None,None,0,Some("C#")),
      Posting(2,5494879,None,Some(5484340),1,None),
      Posting(1,9419744,None,None,2,Some("Objective-C")),
      Posting(1,26875732,None,None,1,Some("C#")),
      Posting(1,9002525,None,None,2,Some("C++")),
      Posting(2,9003401,None,Some(9002525),4,None),
      Posting(2,9003942,None,Some(9002525),1,None),
      Posting(2,9005311,None,Some(9002525),0,None)
    ))
    val scoredPosts = testObject.scoredPostings(testObject.groupedPostings(postings)).collect().toList
    assert(scoredPosts ==
      List((Posting(1,5484340,None,None,0,Some("C#")),1),
        (Posting(1,9002525,None,None,2,Some("C++")),4)
      )
    )
  }

  test("clusterResults") {

    val vectors = StackOverflow.sc.parallelize(List( (450000, 39),(500000, 31),(150000,1),(150000,10),(500000, 55),(150000,2) ,(150000,22)))
    val means = Array((500000, 13),(150000,10))
    var results: Array[(String, Double, Int, Int)] = testObject.clusterResults(means, vectors)

    testObject.printResults(results)

    println(results(0))
    println(results(1))

    assert(results.contains("Python", 100.0, 4, 6)) //I like python~!
    assert(results.contains("Scala", 66.66666666666666, 3,39))
  }

  test("clusterResults2"){
    val centers = Array((0,0), (100000, 0))
    val rdd = StackOverflow.sc.parallelize(List(
      (0, 1000),
      (0, 23),
      (0, 234),
      (0, 0),
      (0, 1),
      (0, 1),
      (50000, 2),
      (50000, 10),
      (100000, 2),
      (100000, 5),
      (100000, 10),
      (200000, 100)  ))
    rdd.map(p => (StackOverflow.findClosest(p, centers), p)).groupByKey().collect().foreach(println)
    testObject.printResults(testObject.clusterResults(centers, rdd))
  }

  test("median") {
    val array1 = Array(1,2,3,4,5)
    val array2 = Array(10)
    val array3 = Array(30,30,40)
    val array4 = Array(300,400)

    assert(StackOverflow.findMedian(array1) == 3)
    assert(StackOverflow.findMedian(array2) == 10)
    val (lower, upper) = array3.sortWith(_ < _).splitAt(array3.length / 2)
    println("lower")
    lower.foreach(println)
    println("upper")
    upper.foreach(println)
    if (array3.length % 2 == 0) math.round((lower.last.toDouble + upper.head.toDouble) / 2D).toInt else upper.head
    assert(StackOverflow.findMedian(array3) == 30)

    assert(StackOverflow.findMedian(array4) == 350)


  }


}
