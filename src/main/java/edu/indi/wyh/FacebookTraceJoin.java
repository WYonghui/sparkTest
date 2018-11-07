package edu.indi.wyh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class FacebookTraceJoin {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> data1 = context.textFile(args[0]);
        JavaPairRDD<String, String> map1 = data1.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\t", 2);
                return new Tuple2<String, String>(strings[0], strings[1]);
            }
        });

        JavaRDD<String> data2 = context.textFile(args[1]);
        JavaPairRDD<String, String> map2 = data2.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\t", 2);
                return new Tuple2<String, String>(strings[0], strings[1]);
            }
        });
        JavaPairRDD<String, Iterable<String>> group2 = map2.groupByKey();

        JavaPairRDD<String, Tuple2<String, Iterable<String>>> join = map1.join(group2);

        join.saveAsTextFile(args[2]);


    }
}
