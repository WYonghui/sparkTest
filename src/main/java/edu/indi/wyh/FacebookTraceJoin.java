package edu.indi.wyh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;


/**
 * The type Facebook trace join.
 * spark-submit --class edu.indi.wyh.FacebookTraceJoin --master spark://node91:7077 --deploy-mode client \
 --executor-memory 16g --total-executor-cores 32 \
 --name facebookTraceJoin spark-test-1.0-SNAPSHOT.jar hdfs://node91:9000//wyh/testDataSet/fb_256M.tsv \
 hdfs://node91:9000//wyh/testDataSet/fb2_256M.tsv hdfs://node91:9000//wyh/output/facebookTrace/test000
 */
public class FacebookTraceJoin {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) throws Exception {
        final Logger LOG = LoggerFactory.getLogger(FacebookTraceJoin.class);

        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> data1 = context.textFile(args[0]);
        JavaPairRDD<String, String> map1 = data1.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\\W+", 2);
//                if (strings.length < 2) {
//                    LOG.error("something is wrong in data1, the results of split is less than two strings");
//                    return null;
//                }
                return new Tuple2<String, String>(strings[0], strings[1]);
            }
        });

        JavaRDD<String> data2 = context.textFile(args[1]);
        JavaPairRDD<String, String> map2 = data2.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\\W+", 2);
                return new Tuple2<String, String>(strings[0], strings[1]);
            }
        });
        JavaPairRDD<String, Iterable<String>> group2 = map2.groupByKey();

        JavaPairRDD<String, Tuple2<String, Iterable<String>>> join = map1.join(group2);

        join.saveAsTextFile(args[2]);

//        InputStreamReader sr = new InputStreamReader(new FileInputStream("E:\\download\\FB-2010_samples_24_times_1hr_0.tsv"));
//        BufferedReader br = new BufferedReader(sr);
//
//        String line = br.readLine();
//        String[] strs;
//        long l = 0;
//
//        while (line != null) {
////            strs = line.split("\\t+", 2);
//            strs = line.split("\\W+", 2);
//            LOG.info("strs.length = " + strs.length);
//            for (String str : strs) {
//                LOG.info(str);
//            }
//
//            line = br.readLine();
//            l++;
//            if (l == 10) {
//                break;
//            }
//        }
//
//        br.close();
//        sr.close();

    }

}
