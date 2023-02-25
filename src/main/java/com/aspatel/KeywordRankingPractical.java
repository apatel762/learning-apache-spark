package com.aspatel;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Given the input text file, representing the subtitles of a Docker course on Udemy: use Spark to
 * process the data, and gather important keywords.
 */
public class KeywordRankingPractical {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("keyword_ranking").setMaster("local[*]");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      sc.textFile("src/main/resources/subtitles/input.txt")
          .flatMap(KeywordRankingPractical::splitLine)
          .filter(StringUtils::isAlpha)
          .take(10)
          .forEach(System.out::println);
    }
  }

  private static Iterator<String> splitLine(String line) {
    String[] words = line.split(" ");
    return Arrays.stream(words).iterator();
  }
}
