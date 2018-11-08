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

import java.util.List;


/**
 * The type Delete abnormal item.
 * spark-submit --class edu.indi.wyh.DeleteAbnormalItem --master spark://node91:7077 --deploy-mode client \
 * --executor-memory 16g --total-executor-cores 32 \
 * --name deleteAbnormalItem spark-test-1.0-SNAPSHOT.jar hdfs://node91:9000//wyh/testDataSet/fb_256M.tsv \
 * hdfs://node91:9000//wyh/testDataSet/fb2_256M.tsv
 */
public class DeleteAbnormalItem {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
//    public static void main(String[] args) {
////        final Logger LOG = LoggerFactory.getLogger(DeleteAbnormalItem.class);
//
//        SparkConf conf = new SparkConf();
//        JavaSparkContext context = new JavaSparkContext(conf);
//
//        JavaRDD<String> data1 = context.textFile(args[0]);
//        JavaRDD<String> p1 = data1.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) throws Exception {
//                String[] strings = s.split("\t", 2);
//                if (strings.length < 2) {
//                    return false;
//                }
//                return true;
//            }
//        });
//
//        p1.saveAsTextFile(args[1]);

//        List<String> c = p1.collect();

//        JavaRDD<String> crdd = context.parallelize(c);
//        crdd.saveAsTextFile(args[1]);

//    }

    /*
    spark-submit --class edu.indi.wyh.DeleteAbnormalItem --master spark://node91:7077 --deploy-mode client \
 --executor-memory 16g --total-executor-cores 32 \
 --name facebookTraceSplit spark-test-1.0-SNAPSHOT.jar hdfs://node91:9000//wyh/testDataSet/fb_256M.tsv \
 hdfs://node91:9000//wyh/output/facebookTrace/test000
     */
    public static void main(String[] args) {
        final Logger LOG = LoggerFactory.getLogger(DeleteAbnormalItem.class);

        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> data1 = context.textFile(args[0]);

        JavaPairRDD<String, String> map1 = data1.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("\\W+", 2);
                if (strings.length < 2) {
                    LOG.error("something is wrong in data1, the results of split is less than two strings");
                    return new Tuple2<String, String>("data1", "less than two strings");
                }
                return new Tuple2<String, String>(strings[0], strings[1]);
            }
        });

        map1.saveAsTextFile(args[1]);

    }
}
