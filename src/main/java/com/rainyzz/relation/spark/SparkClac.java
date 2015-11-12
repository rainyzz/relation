package com.rainyzz.relation.spark;

import com.rainyzz.relation.core.WordMap;
import com.rainyzz.relation.util.LineReader;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple6;

import java.lang.reflect.Array;
import java.util.*;


public class SparkClac {
    public static final double E = 2.71828182846;
    public static final double U = 0.1;
    public static final double M = 0.15;
    public static final int HALF_WINDOW = 2;
    //private static Set<String> allColumn = new HashSet<>(Arrays.asList("des_c","title_c","keyword_c"));
    private static Set<String> allColumn = new HashSet<>(Arrays.asList("abstract_cn","title_cn","keyword_cn"));
    public static JavaPairRDD calc(JavaRDD<String> lines) {
        // 将输入文件缓存
        lines.cache();

        // 统计词对出现的文档次数
        JavaPairRDD<Tuple2<String, String>,Integer> pairDocCountRDD = lines.flatMapToPair(line->{
                Map<String, String> article = LineReader.readRecord(line);
                Set<String> words = new HashSet<>();

                //allColumn.forEach(col -> words.addAll(Arrays.asList(article.get(col).split(" "))));
                    StringBuffer sb = new StringBuffer();
                    allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));
                    List<Term> terms = ToAnalysis.parse(sb.toString());
                    terms.forEach(term->{
                        if(term.getNatureStr().contains("n") || term.getNatureStr().contains("v")){
                            if(!term.getName().equals(" ")){
                                words.add(term.getName());
                            }
                        }
                        //System.out.println(term);
                    });

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

        // 统计单个词出现的文档个数
        JavaPairRDD<String, Integer> wordDocCountRDD = lines.flatMapToPair(line ->{
                Map<String, String> article = LineReader.readRecord(line);
                Set<String> words = new HashSet<>();

                //allColumn.forEach(col -> words.addAll(Arrays.asList(article.get(col).split(" "))));
                StringBuffer sb = new StringBuffer();
                allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));
                List<Term> terms = ToAnalysis.parse(sb.toString());
                terms.forEach(term->{
                    if(term.getNatureStr().contains("n") || term.getNatureStr().contains("v")){
                        if(!term.getName().equals(" ")){
                            words.add(term.getName());
                        }
                    }
                    //System.out.println(term);
                });

                List<Tuple2<String, Integer>> result = new ArrayList<>();
                words.forEach(w->result.add(new Tuple2<>(w, 1)));

                return result;
        }).reduceByKey((a,b)->a+b);

        //统计词窗内权重
        JavaPairRDD<Tuple2<String,String>, Double> weightRDD = lines.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, Double>() {
            public Iterable<Tuple2<Tuple2<String, String>, Double>> call(String line) {
                Map<String, String> article = LineReader.readRecord(line);

                List<Tuple2<Tuple2<String, String>, Double>> result = new ArrayList<>();

                allColumn.forEach(column -> calcWeight(article, column, result, allColumn));

                return result;
            }
            // LAR Model
            private void calcWeight(Map<String, String> article,String column,List<Tuple2<Tuple2<String, String>, Double>> result,Set<String> allColumn){
                String[] text = article.get(column).split(" ");
                Set<String> otherColumns = new HashSet<>(allColumn);
                otherColumns.remove(column);

                for (int i = 0; i < text.length; i++) {
                    String curWord = text[i];

                    int windowStart = i - HALF_WINDOW < 0 ? 0 : i - HALF_WINDOW;
                    int windowEnd = i + HALF_WINDOW > text.length ? text.length : i + HALF_WINDOW;

                    //对词窗内的词进行统计 windowWeight
                    for (int j = windowStart; j < windowEnd; j++) {
                        if (curWord.equals(text[j])) {
                            continue;
                        }
                        int distance = Math.abs(i - j);
                        double weight = U * Math.pow(E, -U * distance);
                        result.add(new Tuple2<>(new Tuple2<>(curWord, text[j]), weight));
                    }
                    //对于当前词，其与所有其他字段中词语都有关系。 diffCoCount
                    otherColumns.forEach(col -> otherColumnClac(article.get(col).split(" "), curWord, text.length, result));

                }
            }

            private void otherColumnClac(String[] keywords, String curWord,int curLen, List<Tuple2<Tuple2<String, String>, Double>> result) {

                int otherLen = keywords.length;

                for (String keyword : keywords) {
                    if (curWord.equals(keyword)) {
                        continue;
                    }

                    if (otherLen == 0) {
                        continue;
                    }
                    double weight = U * Math.pow(E, -U * (M * curLen / otherLen));
                    result.add(new Tuple2<>(new Tuple2<>(curWord, keyword), weight));
                }
            }
        }).reduceByKey((a, b) -> a + b);

        // 统计词语出现次数
        JavaPairRDD<String, Integer> wordCountRDD = lines.flatMapToPair(line -> {
            Map<String, String> article = LineReader.readRecord(line);
            List<String> allWords = new ArrayList<>();

            allColumn.forEach(col->allWords.addAll(Arrays.asList(article.get(col).split(" "))));
            List<Tuple2<String, Integer>> result = new ArrayList<>();
            allWords.forEach(word->result.add(new Tuple2<>(word, 1)));

            return result;

        }).reduceByKey((a, b) -> a + b);

        // 将所有前面的字段值结合在一起
        JavaPairRDD<String, Tuple5<String,Integer,Double,Integer,Integer>> allDataRDD =
                pairDocCountRDD.join(weightRDD).mapToPair(
                        (Tuple2<Tuple2<String,String>,Tuple2<Integer, Double>> tp) ->
                             new Tuple2<>(tp._1()._1(), new Tuple3<>(tp._1()._2(), tp._2()._1(), tp._2()._2())))
                .join(wordDocCountRDD)
                .join(wordCountRDD).mapToPair(tp -> new Tuple2<>(
                        tp._1(), new Tuple5<>(tp._2()._1()._1()._1(),
                        tp._2()._1()._1()._2(),
                        tp._2()._1()._1()._3(),
                        tp._2()._2(),
                        tp._2()._1()._2())
                ));

        // 计算最终的函数值
        //return allDataRDD;

        return allDataRDD.mapToPair((Tuple2<String,Tuple5<String,Integer,Double,Integer,Integer>> tp)->{
            int pairDocCount = tp._2()._2();
            double windowWeight = tp._2()._3();
            int wordCount = tp._2()._4();
            int wordDocCount = tp._2()._5();
            double result = 0;
            result = windowWeight / wordCount * (Math.log(pairDocCount * 1.0 / wordDocCount + 1) / Math.log(2));
            return new Tuple2<>(tp._1(),new Tuple6<>(tp._2()._1(),tp._2()._2(),tp._2()._3(),tp._2()._4(),tp._2()._5(),result));
        }).groupByKey().mapToPair(tp -> {
                    ArrayList<Tuple6<String,Integer,Double,Integer,Integer,Double>> list = new ArrayList<>();
                    tp._2().forEach(t -> list.add(t));
                    Collections.sort(list, (a, b) -> ((Double.compare(b._6(), a._6()))));
                    return new Tuple2<>(tp._1(), list.size() > 100 ? list.subList(0, 100) : list);
                });
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Word Relation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputFile = args[0];
        String outputFile = args[1];
        JavaRDD<String> textFile = sc.textFile(inputFile);
        calc(textFile).saveAsTextFile(outputFile);
    }
}


