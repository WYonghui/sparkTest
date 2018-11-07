package edu.indi.wyh;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * yarn jar spark-test-1.0-SNAPSHOT.jar edu.indi.wyh.UploadToHDFS srcPath hdfs://node91:9000//wyh/testDataSet/***
 * yarn jar spark-test-1.0-SNAPSHOT.jar edu.indi.wyh.UploadToHDFS fileAbc hdfs://node91:9000//wyh/testDataSet/fileAbc
 */
public class UploadToHDFS {
    private static Logger LOG = LoggerFactory.getLogger(UploadToHDFS.class);

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        String src = String.valueOf(args[0]);
        String dst = String.valueOf(args[1]);

        try {
            Path local = new Path(src);
            Path fs = new Path(dst);
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.copyFromLocalFile(local, fs);
        } catch (Exception e ){
            LOG.error(e.getMessage());
        }

    }
}
