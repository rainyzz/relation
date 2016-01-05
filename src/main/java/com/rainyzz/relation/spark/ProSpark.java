package com.rainyzz.relation.spark;

import com.rainyzz.relation.util.LineReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple6;

import java.util.*;

/**
 * Created by rainystars on 15/11/29.
 */
public class ProSpark {
    public static JavaPairRDD calc(JavaRDD<String> lines) {
        // 将输入文件缓存
        lines.cache();
        // 统计词对出现的文档次数
        JavaPairRDD<Tuple2<String, String>,Integer> pairDocCountRDD = lines.flatMapToPair(line->{
                    Map<String, String> article = LineReader.readRecord(line);

                    StringBuffer sb = new StringBuffer();
                    SparkClac.allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));
                    //List<Term> terms = ToAnalysis.parse(sb.toString());
                    Set<String> words = SparkClac.getWords(sb.toString());

                    List<Tuple2<Tuple2<String, String>,Integer>> result = new ArrayList<>();
                    for (String wordA : words) {
                        for (String wordB : words) {
                            if (wordA.equals(wordB)) {
                                continue;
                            }
                            result.add(new Tuple2<>(new Tuple2<>(wordA,wordB),1));
                        }
                    }
                    return result;
                }
        ).reduceByKey((a,b)->a+b);

        JavaPairRDD<String, Integer> wordDocCountRDD = lines.flatMapToPair(line ->{
            Map<String, String> article = LineReader.readRecord(line);

            StringBuffer sb = new StringBuffer();
            SparkClac.allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));
            //List<Term> terms = ToAnalysis.parse(sb.toString());
            Set<String> words = SparkClac.getWords(sb.toString());

            List<Tuple2<String, Integer>> result = new ArrayList<>();
            words.forEach(w->result.add(new Tuple2<>(w, 1)));

            return result;
        }).reduceByKey((a,b)->a+b);

        return pairDocCountRDD.mapToPair(
                tp->new Tuple2<>(tp._1()._1(),new Tuple2<>(tp._1()._2(),tp._2()))
        ).join(wordDocCountRDD).mapToPair(tp->{
            String wordA = tp._1();
            String wordB = tp._2()._1()._1();
            int coCount = tp._2()._1()._2();
            int wordCount = tp._2()._2();
            double weight = coCount * 1.0 / wordCount;
            return new Tuple2<>(wordA,new Tuple2<>(wordB,weight));
        }).groupByKey().mapToPair(tp->{
            ArrayList<Tuple2<String,Double>> list = new ArrayList<>();
            tp._2().forEach(t -> list.add(t));
            Collections.sort(list, (a, b) -> ((Double.compare(b._2(), a._2()))));
            return new Tuple2<>(tp._1(), list.size() > 100 ? list.subList(0, 100) : list);
        });

    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Word Relation Pro");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputFile = args[0];
        String outputFile = args[1];
        JavaRDD<String> textFile = sc.textFile(inputFile);
        calc(textFile).saveAsTextFile(outputFile);
    }
}
