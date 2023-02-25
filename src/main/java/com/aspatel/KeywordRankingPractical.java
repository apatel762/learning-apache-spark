package com.aspatel;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Given the input text file, representing the subtitles of a Docker course on Udemy: use Spark to
 * process the data, and gather important keywords.
 */
public class KeywordRankingPractical {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("keyword_ranking").setMaster("local[*]");

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      sc.textFile("src/main/resources/subtitles/input.txt")
          .map(KeywordRankingPractical::stripPunctuation)
          .map(word -> word.toLowerCase(Locale.ENGLISH))
          .flatMap(KeywordRankingPractical::splitLine)
          .filter(StringUtils::isAlpha)
          .filter(Util::isNotBoring)
          .mapToPair(KeywordRankingPractical::toCountable)
          .reduceByKey(Long::sum)
          .mapToPair(KeywordRankingPractical::flipTuple)
          .sortByKey(false)
          .take(10)
          .forEach(System.out::println);
    }
  }

  private static Iterator<String> splitLine(String line) {
    String[] words = line.split(" ");
    return Arrays.stream(words).iterator();
  }

  private static String stripPunctuation(String word) {
    return word.replaceAll("\\p{Punct}", "");
  }

  /**
   * Turn an object into a tuple that can be counted using {@link
   * org.apache.spark.api.java.JavaPairRDD#reduceByKey}.
   *
   * @param obj An object
   * @param <KEY> Any type
   * @return A tuple containing the object, and the number 1
   */
  private static <KEY> Tuple2<KEY, Long> toCountable(KEY obj) {
    return new Tuple2<>(obj, 1L);
  }

  private static <KEY, VALUE> Tuple2<VALUE, KEY> flipTuple(Tuple2<KEY, VALUE> pair) {
    return new Tuple2<>(pair._2, pair._1);
  }
}
