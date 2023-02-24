package com.aspatel;

import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

  public static void main(String[] args) {
    List<Double> inputData = new Random().doubles(10_000L).boxed().toList();

    SparkConf conf = new SparkConf().setAppName("learning_spark").setMaster("local[*]");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      JavaRDD<Double> myRdd = sc.parallelize(inputData);
      myRdd.take(10).forEach(number -> System.out.println("number = " + number));
    }
  }
}
