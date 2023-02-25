package com.aspatel;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MainSection6 {

  public static void main(String[] args) {
    List<String> logMessages = new ArrayList<>(6);
    logMessages.add("WARN: Tuesday 4 September 0405");
    logMessages.add("WARN: Tuesday 4 September 0406");
    logMessages.add("ERROR: Tuesday 4 September 0408");
    logMessages.add("FATAL: Wednesday 5 September 1632");
    logMessages.add("ERROR: Friday 7 September 1854");
    logMessages.add("WARN: Saturday 8 September 1942");

    SparkConf conf = new SparkConf().setAppName("learning_spark").setMaster("local[*]");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      sc.parallelize(logMessages)
          .mapToPair(
              msg -> {
                String[] split = msg.split(": ");
                String logLevel = split[0];
                return new Tuple2<>(logLevel, 1L);
              })
          .reduceByKey(Long::sum)
          .foreach(pair -> System.out.println(pair._2 + " " + pair._1));
    }
  }
}
