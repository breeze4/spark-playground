package basics;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class CustomerOrders {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        JavaSparkContext sc = new JavaSparkContext("local", "CustomerOrders",
                "D:\\spark\\spark-1.6.0-bin-hadoop2.6", new String[]{"target/spark-coursework-1.0.jar"});

        String file = "src/main/resources/customer-orders.csv";
        JavaRDD<String> distData = sc.textFile(file);

        JavaPairRDD<Double, Integer> customerPurchaseTotal = distData
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    int customerId = Integer.parseInt(fields[0]);
                    Double spentAmount = Double.parseDouble(fields[2]);
                    return new Tuple2<>(customerId, spentAmount);
                })
                .reduceByKey((order1, order2) -> Math.round((order1 + order2) * 100) / 100.0)
                .mapToPair(t -> new Tuple2<>(t._2(), t._1()))
                .sortByKey();

        Map<Double, Integer> customerPurchases = customerPurchaseTotal.collectAsMap();
        new TreeMap<>(customerPurchases).entrySet().forEach(entry -> {
            System.out.println(String.format("(%s, %s)", entry.getValue(), entry.getKey()));
        });
    }
}
