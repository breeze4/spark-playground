package basics;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static java.util.stream.Collectors.toMap;

public class PopularMovies {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        JavaSparkContext sc = new JavaSparkContext("local", "PopularMovies",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        // not allowed to distribute this file - can be obtained from
        // http://grouplens.org/datasets/movielens/ 100k movie rating dataset
        String ratingsDataFile = "src/main/resources/u.data";
        String movieInfoFile = "src/main/resources/u.item";
        Map<Integer, String> movieNames = buildMovieNames(movieInfoFile);

        // broadcast the data once to the cluster nodes
        Broadcast<Map<Integer, String>> movieNamesVar = sc.broadcast(movieNames);

        JavaRDD<String> distData = sc.textFile(ratingsDataFile);
        JavaPairRDD<Integer, Integer> movieIdToCount = distData.mapToPair(entry -> {
            String[] split = entry.split("\\s+");
            Integer movieId = Integer.parseInt(split[1]);
            return new Tuple2<>(movieId, 1);
        }).reduceByKey((running, count) -> running + count);

        // reverse the tuple and replace the ID with name, then sort by the count
        Map<Integer, String> sortedCountsToMovieId = movieIdToCount
                .mapToPair(t -> new Tuple2<>(t._2(), movieNamesVar.getValue().get(t._1())))
                .sortByKey().collectAsMap();

        new TreeMap<>(sortedCountsToMovieId).entrySet().forEach(entry -> {
            System.out.println(String.format("%s count: %s", entry.getValue(), entry.getKey()));
        });
    }

    private static Map<Integer, String> buildMovieNames(String fileName) {
        try (BufferedReader br = Files.newBufferedReader(Paths.get(fileName), StandardCharsets.ISO_8859_1)) {
            return br.lines().map(line -> line.split("\\|")).collect(toMap(
                    splitLine -> Integer.parseInt(splitLine[0]),
                    splitLine -> splitLine[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }
}
