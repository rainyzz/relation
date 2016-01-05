package com.rainyzz.relation.spark;

import com.rainyzz.relation.util.LineReader;
import com.rainyzz.relation.util.Util;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple6;
import sun.plugin2.util.SystemUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class SparkClacGeo {



    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Word Relation Geo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputDir = args[0];
        String outputDir = args[1];
        Configuration hdoopConf = new Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(hdoopConf);
            Path inputPath =new Path(inputDir);
            RemoteIterator<LocatedFileStatus> stats = hdfs.listFiles(inputPath,false);
            while(stats.hasNext()){
                LocatedFileStatus lfs = stats.next();
                String filename = lfs.getPath().getName();
                if(filename.startsWith(".")){
                    continue;
                }

                JavaRDD<String> textFile = sc.textFile(lfs.getPath().toString());
                SparkClac.calc(textFile).saveAsTextFile(outputDir+"/"+filename);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*List<File> files = Util.getListFiles(inputDir);
        String outputDir = args[1];
        for(File f:files){
            if(f.getName().startsWith(".")){
                continue;
            }
            JavaRDD<String> textFile = sc.textFile(inputDir+"/"+f.getName());
            SparkClac.calc(textFile).saveAsTextFile(outputDir+"/"+f.getName());
        }*/
    }
}


