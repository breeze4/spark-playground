package basics;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class WordCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        JavaSparkContext sc = new JavaSparkContext("local", "WordCount",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        String file = "src/main/resources/Book.txt";
//        String file = "src/main/resources/hamlet.txt";

        JavaRDD<String> distData = sc.textFile(file);

        // total word count
//        Accumulator<Integer> accum = sc.accumulator(0);
//        distData.flatMap(s -> Arrays.asList(s.split("\\s+"))).foreach(x -> accum.add(1));
//        long count = accum.value();

        // count by word
        JavaPairRDD<Integer, String> wordCounts = distData
                .flatMap(s -> Arrays.asList(s.split("\\s+")))
                // lower case and remove non-words - not great
                .map(s -> s.toLowerCase().replaceAll("[^\\w\\s]", ""))
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((s1, s2) -> s1 + s2)
                .mapToPair(t -> new Tuple2<>(t._2(), t._1()))
                .sortByKey();
        Map<Integer, String> wordCountsSorted = wordCounts.collectAsMap();
        new TreeMap<>(wordCountsSorted).entrySet().forEach(entry -> {
            System.out.println(String.format("(%s, %s)", entry.getValue(), entry.getKey()));
        });
    }
}
