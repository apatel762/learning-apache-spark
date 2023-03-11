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
      /*
      EXERCISE 1
      Build an RDD containing a key of courseId together with the number of chapters
      on that course.
      */
      System.out.println("EXERCISE 1");
      System.out.println("(no output)");

      JavaPairRDD<String, Long> courseIdAndChapterCount =
          sc.textFile("src/main/resources/viewing figures/chapters.csv")
              .mapToPair(
                  s -> {
                    String[] mapping = s.split(",");
                    return new Tuple2<>(mapping[1], 1L);
                  })
              .reduceByKey(Long::sum);

      /*
      EXERCISE 2
      Produce a ranking chart detailing which are the most popular courses by score.

      We think that if a user sticks it through most of the course, that's more
      deserving of "points" than if someone bails out just a quarter way through the
      course. So we've cooked up the following scoring system:

       - If a user watches more than 90% of the course, the course gets 10 points
       - If a user watches > 50% but <90% , it scores 4
       - If a user watches > 25% but < 50% it scores 2
       - Less than 25% is no score
      */
      System.out.println();
      System.out.println("EXERCISE 2");

      JavaPairRDD<String, String> chaptersToUsers =
          sc.textFile("src/main/resources/viewing figures/views-*.csv")
              .mapToPair(
                  line -> {
                    String[] values = line.split(",");
                    String userId = values[0];
                    String chapterId = values[1];
                    return new Tuple2<>(chapterId, userId);
                  })
              // a user may watch the same chapter multiple times, so must make
              // sure that we are looking at distinct entries in the data set
              .distinct();

      JavaPairRDD<String, String> chaptersToCourses =
          sc.textFile("src/main/resources/viewing figures/chapters.csv")
              .mapToPair(
                  line -> {
                    String[] values = line.split(",");
                    String chapterId = values[0];
                    String courseId = values[1];
                    return new Tuple2<>(chapterId, courseId);
                  });

      // This dataset contains the number of views each user has given to a course.
      JavaPairRDD<Tuple2<String, String>, Long> userCourseViews =
          chaptersToUsers
              .join(chaptersToCourses)
              .mapToPair(
                  row -> {
                    // Unpacking the joined row...
                    String userId = row._2._1;
                    String courseId = row._2._2;

                    // Think of the new data structure like this:
                    // For "Course", "User" has added one view
                    return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
                  })
              .reduceByKey(Long::sum);

      // TODO: join user course views with courseIdAndChapterCount somehow to figure
      //  figure out how many chapters a course has, and from there, we can start
      //  calculating scores for the courses.

      /*
      EXERCISE 3
      This is optional but as a final touch, we can get the titles using a final
      (small data) join. You can also sort by total score (descending) to get the
      results in a pleasant format.
      */
      System.out.println();
      System.out.println("EXERCISE 3");

      /*
      EXERCISE 4
      Once you've got the output looking "nice", it's more fun to run the job against
      some real data - change the value for testMode to false, and it will read the data
      from disk (using some anonymized real viewing figures from
      VirtualPairProgrammers.com, over a short period from last year).
      */
      System.out.println();
      System.out.println("EXERCISE 4");
    }
  }

  private static <KEY, VALUE> Tuple2<VALUE, KEY> flipTuple(Tuple2<KEY, VALUE> pair) {
    return new Tuple2<>(pair._2, pair._1);
  }
}
