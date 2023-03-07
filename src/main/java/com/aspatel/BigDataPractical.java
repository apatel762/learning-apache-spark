package com.aspatel;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Given the input text file, representing the subtitles of a Docker course on Udemy: use Spark to
 * process the data, and gather important keywords.
 */
public class BigDataPractical {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("big_data").setMaster("local[*]");

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      // build an RDD containing a key of courseId together
      // with the number of chapters on that course.
      JavaPairRDD<String, Long> courseIdAndChapterCount =
          sc.textFile("src/main/resources/viewing figures/chapters.csv")
              .mapToPair(
                  s -> {
                    String[] mapping = s.split(",");
                    return new Tuple2<>(mapping[1], 1L);
                  })
              .reduceByKey(Long::sum)
              .sortByKey();
      courseIdAndChapterCount.foreach(
          o -> System.out.println("Course " + o._1 + " has " + o._2 + " chapters"));
    }
  }

  private static <KEY, VALUE> Tuple2<VALUE, KEY> flipTuple(Tuple2<KEY, VALUE> pair) {
    return new Tuple2<>(pair._2, pair._1);
  }
}
