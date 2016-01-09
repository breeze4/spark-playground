package basics;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AverageFriendsAge {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        JavaSparkContext sc = new JavaSparkContext("local", "AverageFriendsAge",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        String file = "src/main/resources/fakefriends.csv";
        JavaRDD<String> rdd = sc.textFile(file);
        JavaPairRDD<Integer, Integer> ageFriendPairs = rdd.mapToPair(line -> {
            String[] s = line.split(",");
            int age = Integer.parseInt(s[2]);
            int numFriends = Integer.parseInt(s[3]);
            return new Tuple2<>(age, numFriends);
        });
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> ageToFriendsAndNumEntries = ageFriendPairs
                // add tuple for each age with value 1
                .mapValues(friends -> new Tuple2<>(friends, 1))
                // add up number of friends and add up number of people of that age
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()));

        JavaPairRDD<Integer, Integer> averagesByAge = ageToFriendsAndNumEntries
                .sortByKey()
                .mapValues(t -> t._1() / t._2());

        List<Tuple2<Integer, Integer>> averageFriendsByAge = averagesByAge.collect();
        averageFriendsByAge.stream().forEach(t -> {
            System.out.println(t._1() + ", " + t._2());
        });
    }
}
