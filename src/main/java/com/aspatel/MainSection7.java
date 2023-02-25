package com.aspatel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MainSection7 {

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
          .flatMap(msg -> Arrays.stream(msg.split(" ")).iterator())
          .filter(word -> !word.contains(":"))
          .distinct()
          .foreach(word -> System.out.println(word));
    }
  }
}
