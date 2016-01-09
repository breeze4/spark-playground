package basics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import java.util.TreeMap;

public class RatingsCounter {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        // not allowed to distribute this file - can be obtained from
        // http://grouplens.org/datasets/movielens/ 100k movie rating dataset
        String logFile = "src/main/resources/u.data";
        JavaSparkContext sc = new JavaSparkContext("local", "RatingsCounter",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        JavaRDD<String> lines = sc.textFile(logFile).cache();

        Map<String, Long> countsByValue = lines.map((s) -> s.split("\\s+")[2]).countByValue();
        Map<String, Long> sortedCounts = new TreeMap<>(countsByValue);
        System.out.println(sortedCounts);
    }
}