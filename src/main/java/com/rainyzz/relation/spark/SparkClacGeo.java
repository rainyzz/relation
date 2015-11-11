package com.rainyzz.relation.spark;

import com.rainyzz.relation.util.LineReader;
import com.rainyzz.relation.util.Util;
import org.apache.commons.io.FileUtils;
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
import java.util.*;


public class SparkClacGeo {



    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Word Relation Geo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputDir = args[0];

        List<File> files = Util.getListFiles(inputDir);
        String outputDir = args[1];
        for(File f:files){
            if(f.getName().startsWith(".")){
                continue;
            }
            JavaRDD<String> textFile = sc.textFile(inputDir+"/"+f.getName());
            SparkClac.calc(textFile).saveAsTextFile(outputDir+"/"+f.getName());
        }
    }
}


