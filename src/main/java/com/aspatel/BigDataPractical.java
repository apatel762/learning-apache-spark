package com.aspatel;

import static com.google.common.base.Preconditions.*;
import static java.util.Comparator.*;

import java.util.Map.Entry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

      JavaRDD<String> chapters = sc.textFile("src/main/resources/viewing figures/chapters.csv");
      JavaRDD<String> views = sc.textFile("src/main/resources/viewing figures/views-*.csv");
      JavaRDD<String> titles = sc.textFile("src/main/resources/viewing figures/titles.csv");

      /*
      EXERCISE 1
      Build an RDD containing a key of courseId together with the number of chapters
      on that course.
      */
      System.out.println("EXERCISE 1");
      System.out.println("(no output)");

      JavaPairRDD<String, Long> courseIdAndChapterCount =
          chapters
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
      System.out.println("(no output)");

      JavaPairRDD<String, String> chaptersToUsers =
          views
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
          chapters.mapToPair(
              line -> {
                String[] values = line.split(",");
                String chapterId = values[0];
                String courseId = values[1];

                return new Tuple2<>(chapterId, courseId);
              });

      JavaPairRDD<String, Integer> coursesAndViews =
          chaptersToUsers
              .join(chaptersToCourses)
              .mapToPair(
                  row -> {
                    String userId = row._2._1;
                    String courseId = row._2._2;

                    // Think of the new data structure like this:
                    // For a given "Course", "User" has added "1" view
                    return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
                  })
              .reduceByKey(Long::sum)
              .mapToPair(
                  row -> {
                    String userId = row._1._1;
                    String courseId = row._1._2;
                    long chaptersSeen = row._2;

                    // For a given "Course", each "User" has seen "x" chapters.
                    return new Tuple2<>(courseId, new Tuple2<>(userId, chaptersSeen));
                  })
              .join(courseIdAndChapterCount)
              .mapToPair(
                  row -> {
                    String courseId = row._1;
                    long chaptersSeen = row._2._1._2;
                    long chapterCount = row._2._2;

                    int score = calculateScore(chaptersSeen, chapterCount);

                    return new Tuple2<>(courseId, score);
                  })
              .reduceByKey(Integer::sum);

      /*
      EXERCISE 3
      This is optional but as a final touch, we can get the titles using a final
      (small data) join. You can also sort by total score (descending) to get the
      results in a pleasant format.
      */
      System.out.println();
      System.out.println("EXERCISE 3");
      titles
          .mapToPair(
              s -> {
                String[] mapping = s.split(",");
                String courseId = mapping[0];
                String title = mapping[1];
                return new Tuple2<>(courseId, title);
              })
          .join(coursesAndViews)
          .mapToPair(
              row -> {
                String title = row._2._1;
                int totalViews = row._2._2;

                return new Tuple2<>(totalViews, title);
              })
          .collectAsMap()
          .entrySet()
          .stream()
          .sorted(Entry.<Integer, String>comparingByKey().reversed())
          .forEachOrdered(
              entry -> {
                int totalViews = entry.getKey();
                String title = entry.getValue();

                System.out.println(totalViews + " " + title);
              });

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

  /**
   * Calculate the score to give to a course based on the number of chapters that a user has seen in
   * proportion to the total number of chapters.
   *
   * <p>Our formula is as follows:
   *
   * <ul>
   *   <li>If a user watches more than 90% of the course, the course gets 10 points
   *   <li>If a user watches > 50% but <90% , it scores 4
   *   <li>If a user watches > 25% but < 50% it scores 2
   *   <li>Less than 25% is no score
   * </ul>
   *
   * @param chaptersSeen The distinct number of chapters that a user has viewed. This must be a
   *     non-negative number.
   * @param chapterCount The total number of chapters in the course. This must be a non-negative
   *     number, that is greater than or equal to the view count.
   * @return A score for the course, calculated using our formula.
   */
  private static int calculateScore(long chaptersSeen, long chapterCount) {
    checkState(chapterCount >= chaptersSeen);
    checkState(chapterCount >= 0);
    checkState(chaptersSeen >= 0);

    double percentageViewed = ((double) chaptersSeen / (double) chapterCount) * 100;

    if (percentageViewed < 25d) {
      return 0;
    } else if (percentageViewed < 50d) {
      return 2;
    } else if (percentageViewed < 90d) {
      return 4;
    } else {
      return 10;
    }
  }
}
