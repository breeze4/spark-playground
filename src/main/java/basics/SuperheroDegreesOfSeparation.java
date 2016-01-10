package basics;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static basics.SuperheroDegreesOfSeparation.NodeVisited.COMPLETED;
import static basics.SuperheroDegreesOfSeparation.NodeVisited.PROCESSED;
import static basics.SuperheroDegreesOfSeparation.NodeVisited.UNPROCESSED;

public class SuperheroDegreesOfSeparation {

    static {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
    }

    enum NodeVisited {
        UNPROCESSED, PROCESSED, COMPLETED;
    }

    private static final Integer START_ID = 5306; // spiderman
    private static final Integer TARGET_ID = 14; // "Adam 3,031"

    private static final JavaSparkContext sc = new JavaSparkContext("local", "SuperheroDegreesOfSeparation",
            "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});
    private static final Accumulator<Integer> hitCounter = sc.accumulator(0);

    public static void main(String[] args) {
        String superheroGraph = "src/main/resources/Marvel-Graph.txt";

        JavaRDD<String> inputFile = sc.textFile(superheroGraph);
        JavaPairRDD<Integer, Tuple3<List<Integer>, Integer, NodeVisited>> graphRDD =
                inputFile.mapToPair(SuperheroDegreesOfSeparation::convertToBFSNode);

        for(int i = 1; i <= 10; i++) {
            System.out.println("Running BFS iteration #: " + i);

            JavaPairRDD<Integer, Tuple3<List<Integer>, Integer, NodeVisited>> mapped =
                    graphRDD.flatMapToPair(SuperheroDegreesOfSeparation::visitNode);
            long count = mapped.count();
            System.out.println("Processed: " + count + " nodes");
            if(hitCounter.value() > 0) {
                System.out.println("Hit the target hero! From " + hitCounter.value() + " different directions");
                break;
            }

            graphRDD = mapped.reduceByKey(SuperheroDegreesOfSeparation::reduce);
        }
    }

    private static Tuple2<Integer, Tuple3<List<Integer>, Integer, NodeVisited>> convertToBFSNode(String line) {
        String[] fields = line.split("\\s+");
        Integer heroId = Integer.parseInt(fields[0]);
        List<Integer> connections = Arrays.stream(fields)
                .skip(1)
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        NodeVisited status = UNPROCESSED;
        Integer distance = 9999;
        if (heroId.equals(START_ID)) {
            status = PROCESSED;
            distance = 0;
        }
        return new Tuple2<>(heroId, new Tuple3<>(connections, distance, status));
    }

    private static Iterable<Tuple2<Integer, Tuple3<List<Integer>, Integer, NodeVisited>>>
    visitNode(Tuple2<Integer, Tuple3<List<Integer>, Integer, NodeVisited>> node) {
        Integer heroId = node._1();
        List<Integer> connections = node._2()._1();
        Integer distance = node._2()._2();
        NodeVisited status = node._2()._3();

        List<Tuple2<Integer, Tuple3<List<Integer>, Integer, NodeVisited>>> results = new ArrayList<>();
        if (status.equals(PROCESSED)) {
            for (Integer connectionId : connections) {
                Integer newHeroId = connectionId;
                Integer newDistance = distance + 1;
                NodeVisited newStatus = PROCESSED;

                if (newHeroId.equals(TARGET_ID))
                    hitCounter.add(1);

                results.add(new Tuple2<>(newHeroId, new Tuple3<>(Collections.emptyList(), newDistance, newStatus)));
            }
            status = COMPLETED;
        }
        results.add(new Tuple2<>(heroId, new Tuple3<>(connections, distance, status)));
        return results;
    }

    private static Tuple3<List<Integer>, Integer, NodeVisited> reduce(Tuple3<List<Integer>, Integer, NodeVisited> node1,
                                                                      Tuple3<List<Integer>, Integer, NodeVisited> node2) {
        List<Integer> edges1 = node1._1();
        List<Integer> edges2 = node2._1();
        Integer distance1 = node1._2();
        Integer distance2 = node2._2();
        NodeVisited status1 = node1._3();
        NodeVisited status2 = node2._3();

        Integer distance = 9999;
        NodeVisited status = UNPROCESSED;
        List<Integer> edges = new ArrayList<>();

        // see if one is the original node with its connections, if so preserve them
        if (edges1.size() > 0)
            edges.addAll(edges1);
        else if (edges2.size() > 0)
            edges.addAll(edges2);

        // pick the minimum distance
        distance = distance1 < distance2 ? distance1 : distance2;
        // pick the status that is closer towards completion
        status = pickStatuses(status1, status2);

        return new Tuple3<>(edges, distance, status);
    }

    private static NodeVisited pickStatuses(NodeVisited status1, NodeVisited status2) {
        if (status1 == UNPROCESSED && (status2 == PROCESSED || status2 == COMPLETED))
            return status2;

        if (status1 == PROCESSED && status2 == COMPLETED)
            return status2;

        if (status2 == UNPROCESSED && (status1 == PROCESSED || status1 == COMPLETED))
            return status1;

        if (status2 == PROCESSED && status1 == COMPLETED)
            return status1;
        return UNPROCESSED;
    }
}
