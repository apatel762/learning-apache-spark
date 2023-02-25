package com.aspatel;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class KeywordRankingPractical {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("keyword_ranking").setMaster("local[*]");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      sc.textFile("src/main/resources/subtitles/input.txt")
          .take(10)
          .forEach(line -> System.out.println(line));
    }
  }
}
