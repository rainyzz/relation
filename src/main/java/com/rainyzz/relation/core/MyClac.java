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
    public static final String[] filterWords ={"算法","应用","问题","结果","任务","特点","概念","存在","实际",
        "方法","分析","探讨","采用","技术","发展","研究","相关","影响","提供","中的","利用","工作","因素","综合","提高","建立","作用","系统","情况","一种","本文","关系","建设","提出","变化","针对","管理","时间","水平","环境","我国","模式","结构","效果","措施","设计","质量","基于","基础上","一个","方式","参考","信息","现状","能力","介绍","过程中","建议","体系","选择","10","解决","特征","功能","经济","调查","对策","理论","检测","降低","2011","工程","差异","创新","企业","过程","结论","控制","发生","生产","社会","传统","资源","数据","计算","实践","12","15","价值","文章","20","产业","30","中国","趋势","组织","2010","目标","优化","意义","效率","需求","目的","模型","评价","条件","施工","增加","指标","阐述","程度","发现","地区","领域","11","机制","治疗","国家","运行","学生","教育","医院","为例","进一步","实验","培养","增长","分布","特性","资料","开发","临床","之间","教学","检查","原因","政府","诊断","服务","quot","平均","试验","浅谈","市场","完善","提升","重要的","基础","方案","网络","学习","性能","检验","实施","样本","农业","构建","政策","城市"};

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
                if(term.getNatureStr().contains("n") /*|| term.getNatureStr().contains("v")*/){
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
            Count count = frenquecy.get(wordA);
            Count newCount = new Count();
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
