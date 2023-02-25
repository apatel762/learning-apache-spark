package com.aspatel;

import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

  public static void main(String[] args) {
    List<Integer> inputData = new Random().ints(10L).boxed().toList();

    SparkConf conf = new SparkConf().setAppName("learning_spark").setMaster("local[*]");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      sc.parallelize(inputData).map(Math::sqrt).foreach(i -> System.out.println("i = " + i));
    }
  }
}
