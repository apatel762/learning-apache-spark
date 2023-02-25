# learning-apache-spark

The code that I've written while playing around with Apache Spark.

[This](https://www.udemy.com/course/apache-spark-for-java-developers) is the course that I'm working on.

## Notes

To make Spark work with Java 17, you need to include the following JVM argument when you are running your Spark application:

```
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.base/sun.security.action=ALL-UNNAMED
```

See "[Running unit tests with Spark 3.3.0 on Java 17 fails...](https://stackoverflow.com/questions/72724816/running-unit-tests-with-spark-3-3-0-on-java-17-fails-with-illegalaccesserror-cl)".
