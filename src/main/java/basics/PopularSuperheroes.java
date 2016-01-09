package basics;

        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.broadcast.Broadcast;
        import scala.Tuple2;

        import java.io.BufferedReader;
        import java.io.IOException;
        import java.io.Serializable;
        import java.nio.charset.StandardCharsets;
        import java.nio.file.Files;
        import java.nio.file.Paths;
        import java.util.Arrays;
        import java.util.Collections;
        import java.util.Comparator;
        import java.util.Map;
        import java.util.TreeMap;
        import java.util.regex.Matcher;
        import java.util.regex.Pattern;
        import java.util.stream.Collectors;

        import static java.util.stream.Collectors.toMap;

public class PopularSuperheroes {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        JavaSparkContext sc = new JavaSparkContext("local", "PopularSuperheroes",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        String superheroGraph = "src/main/resources/Marvel-Graph.txt";
        String superheroNamesFile = "src/main/resources/Marvel-Names.txt";
        Map<Integer, String> superheroNames = buildSuperheroNames(superheroNamesFile);
        Broadcast<Map<Integer, String>> superheroNamesVar = sc.broadcast(superheroNames);

        JavaPairRDD<Integer, Integer> heroIdToCount = sc.textFile(superheroGraph)
                .flatMap(line -> {
                    String[] split = line.split("\\s+");
                    return Arrays.stream(split).skip(1).collect(Collectors.toList());
                })
                .map(Integer::parseInt)
                .mapToPair(heroId -> new Tuple2<>(heroId, 1))
                .reduceByKey((sum, count) -> sum + count);

        JavaPairRDD<Integer, Integer> flipped = heroIdToCount
                .mapToPair(t -> new Tuple2<>(t._2(), t._1()));
        // cast with multiple interfaces, thinks it can't serialize unless the comparator implements Serializable
        Comparator<Tuple2<Integer, Integer>> tuple2Comparator = (Comparator<Tuple2<Integer, Integer>> & Serializable)
                (t1, t2) -> t1._1().compareTo(t2._1());
        Tuple2<Integer, Integer> winner = flipped.max(tuple2Comparator);

        System.out.println(String.format("%s count: %s", superheroNamesVar.getValue().get(winner._2()), winner._1()));
    }

    private static Map<Integer, String> buildSuperheroNames(String fileName) {
        final Pattern CAPTURE_BETWEEN_QUOTES = Pattern.compile("\"(.+?)\"");

        try (BufferedReader br = Files.newBufferedReader(Paths.get(fileName), StandardCharsets.ISO_8859_1)) {
            return br.lines().map(line -> line.split("\\s+", 2)).collect(toMap(
                    splitLine -> Integer.parseInt(splitLine[0]),
                    splitLine -> {
                        Matcher matcher = CAPTURE_BETWEEN_QUOTES.matcher(splitLine[1]);
                        // there's an entry - an unnamed superhero?: 4656 ""
                        return matcher.find() ? matcher.group() : "";
                    }));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }
}
