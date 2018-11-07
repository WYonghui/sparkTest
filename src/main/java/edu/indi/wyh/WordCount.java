package edu.indi.wyh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*
 * spark-submit --class edu.indi.wyh.WordCount --master spark://node91:7077 --deploy-mode client \
--executor-memory 16g --total-executor-cores 32 \
--name wordcount spark-test-1.0-SNAPSHOT.jar wordcount spark://node91:7077 \
hdfs://node91:9000//wyh/testDataSet/enwiki-20181020-pages-articles1.xml hdfs://node91:9000//wyh/output/wordcounttest/wordcount
 */

public class WordCount {

    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName(String.valueOf(args[0])).setMaster(String.valueOf(args[1]));
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(String.valueOf(args[2]));

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

        JavaRDD<String> wordsList = counts.map(new Function<Tuple2<String, Integer>, String>() {
            public String call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._1;
            }
        });

//        wordsList.saveAsTextFile(String.valueOf(args[3]));  //提取所有word，作为测试时map的输入

        counts.saveAsTextFile(String.valueOf(args[3]));


//        List<Tuple2<String, Integer>> output = counts.collect();
//        for (Tuple2<?,?> tuple : output){
//
//        }

    }
}
