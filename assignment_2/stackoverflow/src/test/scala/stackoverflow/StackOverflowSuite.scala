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

  import StackOverflow._
  test("groupedPosting groups answers and questions together") {
    val postings = sc.parallelize(List(
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
    val groupedPosts = groupedPostings(postings).collect().toList
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
    val postings = sc.parallelize(List(
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
    val scoredPosts = scoredPostings(groupedPostings(postings)).collect().toList
    assert(scoredPosts ==
      List((Posting(1,5484340,None,None,0,Some("C#")),1),
        (Posting(1,9002525,None,None,2,Some("C++")),4)
      )
    )
  }


}
