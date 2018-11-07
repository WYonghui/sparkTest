package edu.indi.wyh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/*
 * spark-submit --class edu.indi.wyh.WordCountTest --master spark://node91:7077 --deploy-mode client \
--executor-memory 16g --total-executor-cores 32 \
--name wordcount spark-test-1.0-SNAPSHOT.jar wordcount spark://node91:7077 hdfs://node91:9000//wyh/output/wordcounttest/wordcount
 */

public class WordCountTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(String.valueOf(args[0])).setMaster(String.valueOf(args[1]));
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> strs = new ArrayList();
        strs.add("Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and BytesWritable values that contain a serialized partition.");
        strs.add("Returns the index of the first occurrence of the specified element in this list, or -1 if this list does not contain the element.");
        strs.add("Returns a list iterator over the elements in this list (in proper sequence), starting at the specified position in the list.");

        JavaRDD<String> lines = sc.parallelize(strs);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] strs = s.split("\\W+");
                return Arrays.asList(strs).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        counts.saveAsTextFile(String.valueOf(args[2]));
    }
}
