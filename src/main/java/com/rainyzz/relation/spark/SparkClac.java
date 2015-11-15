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
    public static final String[] filterWords ={"方法","分析","探讨","采用","技术","发展","研究","相关","影响","提供","中的","利用","工作","因素","综合","提高","建立","作用","系统","情况","一种","本文","关系","建设","提出","变化","针对","管理","时间","水平","环境","我国","模式","结构","效果","措施","设计","质量","基于","基础上","一个","方式","参考","信息","现状","能力","介绍","过程中","建议","体系","选择","10","解决","特征","功能","经济","调查","对策","理论","检测","降低","2011","工程","差异","创新","企业","过程","结论","控制","发生","生产","社会","传统","资源","数据","计算","实践","12","15","价值","文章","20","产业","30","中国","趋势","组织","2010","目标","优化","意义","效率","需求","目的","模型","评价","条件","施工","增加","指标","阐述","程度","发现","地区","领域","11","机制","治疗","国家","运行","学生","教育","医院","为例","进一步","实验","培养","增长","分布","特性","资料","开发","临床","之间","教学","检查","原因","政府","诊断","服务","quot","平均","试验","浅谈","市场","完善","提升","重要的","基础","方案","网络","学习","性能","检验","实施","样本","农业","构建","政策","城市"};
    //private static Set<String> allColumn = new HashSet<>(Arrays.asList("des_c","title_c","keyword_c"));
    public static Set<String> filterSet = new HashSet<>(Arrays.asList(filterWords));
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
                        if(!term.getName().equals(" ") && !filterSet.contains(term.getName())){
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
                    if(filterSet.contains(curWord)){
                        continue;
                    }

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


