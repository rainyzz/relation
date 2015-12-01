package com.rainyzz.relation.core;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import scala.Tuple2;

import java.util.*;

/**
 * Created by rainystars on 10/23/2015.
 */
public class MyClac

{
    public static final double E = 2.71828182846;
    public static final double U = 0.1;
    public static final double M = 0.15;
    public static final int HALF_WINDOW = 2;
    public static final String[] filterWords ={"主要","进行","研究","通过","具有","分析","以及","方法","影响","提高","应用","重要","结果","有效","作用","不同","同时","采用","技术","提供","情况","基础","过程","探讨","发展","问题","结合","利用","方面","作为","提出","可以","相关","系统","因素","一种","工作","存在","质量","变化","效果","建立","结构","表明","本文","时间","环境","实现","水平","分别","特点","比较","设计","明显","降低","基于","使用","条件","管理","针对","介绍","关系","试验","随着","控制","处理","显著","模型","要求","增加","我国","措施","根据","其中","检测","发生","建设","差异","一个","安全","数据","生产","意义","理论","中国","经济","功能","实验","性能","方式","计算","解决","出现","特征","对照","得到","结论","评价","目的","发现","反应","实际","治疗","工程","能力","参数","达到","综合","高于","优化","企业","含量","诊断","现状","临床","资料","目前","地区","观察","社会","平均","疾病","促进","组织","疗效","患者","选择","特性","分为","程度","成为","及其","统计学","体系","机制","随机","两组","一定","实践","手术","表现","模式","方案","资源","产生","减少","设备","回顾","为了","测定","运行","需要","显示","阐述","并发症","分布","药物","模拟","最后","如何","我院","检查","材料","加强","蛋白","之间","工艺","细胞","健康","生长","产业","信息","温度","症状","指标","创新","原因","正常","感染","验证","软件","对策","运用","改善","教学","医院","可能","传统","建议","算法","表达","免疫","生态","年龄","推广","仿真","严重","一些","市场","浓度"};

    public static Set<String> filterSet = new HashSet<>(Arrays.asList(filterWords));
    private static Set<String> allColumn = new HashSet<>(Arrays.asList("abstract_cn","title_cn","keyword_cn"));

    public static void calc(Map<String,String> article,Count wordCount, Map<Integer,Count> windowWeight,Count wordDocCount,
                            Map<Integer,Count> pariDocCount){

            StringBuffer sb = new StringBuffer();
            allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));
            List<Term> terms = ToAnalysis.parse(sb.toString());
            //将分词中不是keyword的词语去掉
            Set<String> words = new HashSet<>();
            for(Term term:terms) {
                if(term.getNatureStr().contains("n") || term.getNatureStr().contains("v")){
                    if(!term.getName().equals(" ") && !filterSet.contains(term.getName())){
                        words.add(term.getName());
                    }
                }
            }

            Set<Integer> local = Sets.newHashSet();

            for(String column :allColumn){
                String[] text = article.get(column).split(" ");
                Set<String> otherColumns = new HashSet<>(allColumn);
                otherColumns.remove(column);
                for(int i = 0; i <  text.length; i++){

                    String word = text[i];
                    if(!words.contains(word)){
                        continue;
                    }
                    int curWordIndex = WordMap.set(word);

                    local.add(curWordIndex);
                    //计算每个词出现的总次数
                    wordCount.increase(curWordIndex,1);

                    //划分词窗
                    int windowStart = i - HALF_WINDOW < 0 ? 0 : i - HALF_WINDOW;
                    int windowEnd = i + HALF_WINDOW > text.length ? text.length : i + HALF_WINDOW;

                    Count count = null;
                    if(!windowWeight.containsKey(curWordIndex)) {
                        count = new Count();
                    }else{
                        count = windowWeight.get(curWordIndex);
                    }

                    //对词窗内的词进行统计 coCount
                    for (int j = windowStart; j < windowEnd; j++) {
                        String windowWord = text[j];
                        if(word.equals(windowWord)||!words.contains(windowWord)){
                            continue;
                        }
                        // 根据距离，计算相似度
                        int distance = Math.abs(i-j);
                        double weight = U * Math.pow(E,- U * distance);

                        int windowWordIndex = WordMap.set(windowWord);
                        count.increase(windowWordIndex,weight);
                    }

                    //对于当前词，其与所有其他字段中词语都有关系。 diffCoCount
                    int curLen = text.length;
                    for(String otherCol:otherColumns){

                        String[] keywords = article.get(otherCol).split(" ");
                        int keywordLen = keywords.length;

                        for(String key:keywords){
                            if(word.equals(key)|| !words.contains(key)){
                                continue;
                            }
                            int keywordIndex = WordMap.set(key);

                            double weight = U * Math.pow(E,- U * (M * curLen / keywordLen));
                            count.increase(keywordIndex,weight);
                        }
                    }

                    windowWeight.put(curWordIndex,count);
                }
            }


            //计算 F(w)，即一个词出现的文档数
            for(int index : local){
                wordDocCount.increase(index,1);
            }
            //计算词语直接共同出现的文档数 F(w1, w2)
            for(int wordA : local){
                Count count = null;
                if(!pariDocCount.containsKey(wordA)) {
                    count = new Count();
                }else{
                    count = pariDocCount.get(wordA);
                }
                //计算机wordA下所有wordB
                for(int wordB : local){
                    if(wordA == wordB){
                        continue;
                    }
                    count.increase(wordB,1);
                }
                pariDocCount.put(wordA,count);
            }

    }

    public static void update(Map<Integer,Count> frenquecy,Count wordTotalCount, Count wordCount, Map<Integer,Count> wordCoCount){


        Set<Integer> set = new HashSet(frenquecy.keySet());
        for(int wordA:set){
            Count<Integer> count = frenquecy.get(wordA);
            Count<Integer> newCount = new Count<>();
            for(Map.Entry<Integer,Double> entry : count.entrySet()){
                int wordB = entry.getKey();
                double value = 0;
                if(wordCoCount.get(wordA) == null || wordCount.get(wordA) == null || wordCoCount.get(wordA).get(wordB) == null){
                    value = 0;
                }else{
                    value = entry.getValue() / wordTotalCount.get(wordA)
                            * (Math.log(wordCoCount.get(wordA).get(wordB) / wordCount.get(wordA) + 1) / Math.log(2));
                }
                newCount.set(wordB,value);
            }
            frenquecy.put(wordA,newCount);
        }
    }
}
