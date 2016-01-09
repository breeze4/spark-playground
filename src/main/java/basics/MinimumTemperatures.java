package basics;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class MinimumTemperatures {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        JavaSparkContext sc = new JavaSparkContext("local", "MinimumTemperatures",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        String file = "src/main/resources/1800.csv";

        JavaRDD<String> distData = sc.textFile(file);

        Map<String, Double> stationIdToMinTempForYear = distData
                .map(s -> {
                    String[] fields = s.split(",");
                    String stationId = fields[0];
                    String entryType = fields[2];
                    double temperature = Double.parseDouble(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0;
                    double roundedTemp = Math.round(temperature * 100) / 100.0;
                    return new Tuple3<>(stationId, entryType, roundedTemp);
                })
                .filter(t -> "TMIN".equals(t._2()))
                .mapToPair(t -> new Tuple2<>(t._1(), t._3()))
                .reduceByKey((t1, t2) -> Math.min(t1, t2)).collectAsMap();

        stationIdToMinTempForYear.forEach((stationId, minTemp) -> {
            System.out.println(String.format("Station ID: %s, min temp for year 1800: %sF", stationId, minTemp));
        });
    }
}
