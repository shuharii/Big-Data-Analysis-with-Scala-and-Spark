package wikipedia

import org.apache.commons.codec.language.bm.Lang
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("eren").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines.map(x => WikipediaData.parse(x))).persist()

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd.aggregate(0)(
      (a,b) => if (b.mentionsLanguage(lang)) a + 1 else a,
      (c,d) => c + d
    )


    //val combOp = (some, another: (some[1] + another[0], some[1] + another[1]))

    //val sj = rdd.aggregate(0)(seqOp, combOp)

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {

    val unsortedList : List[(String, Int)] = langs.map(lang => (lang, occurrencesOfLang(lang, rdd))) //creating unsorted list
    implicit val sortIntegerByString: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x.compare(y)
    }
    unsortedList.sortBy(- _._2)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    rdd.flatMap(article => articleFilter(article)).groupByKey()
    //rdd.map(article => article.mentionsLanguage(langs.map(lang => (lang))))
    //langs.map(lang => (lang, article))
    //rdd.filter(article => article.mentionsLanguage(lang)) articles filtered which language is used in
    def articleFilter(article: WikipediaArticle): Seq[(String, WikipediaArticle)] =
      langs.filter(lang => article.mentionsLanguage(lang)).map(lang => (lang, article))


  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *   a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  /*
    Hint: You might want to use methods flatMap and groupByKey on RDD for this part.
    Computing the ranking, rankLangsUsingIndex
    Use the makeIndex method implemented in the previous part to implement a faster method for computing the language ranking.
    Like in part 1, rankLangsUsingIndex should compute a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).
    Again, the list should be sorted in descending order. That is, according to this ranking, the pair with the highest second component (the count) should be the first element of the list.
    Hint: method mapValues on PairRDD could be useful for this part.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.map(x => (x._1, x._2.size)).collect().toList.sortBy(x => -x._2)
  }

  /*   (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *   Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *   and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val rdd2 = rdd.flatMap(article => langs.filter(article.mentionsLanguage).map(lang => (lang, 1)))
    rdd2.reduceByKey(_ + _).collect().toList.sortBy(x => -x._2)
  }

  def main(args: Array[String]): Unit = {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
