package com.rainyzz.relation.core;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.glassfish.grizzly.utils.ArraySet;

import java.util.*;

/**
 * Created by rainystars on 10/23/2015.
 */
public class ConCalc {
    private static Set<String> allColumn = new HashSet<>(Arrays.asList("abstract_cn", "title_cn", "keyword_cn"));

    public static final int HALF_WINDOW = 2;
    public static final String[] filterWords ={"方法","分析","探讨","采用","技术","发展","研究","相关","影响","提供","中的","利用","工作","因素","综合","提高","建立","作用","系统","情况","一种","本文","关系","建设","提出","变化","针对","管理","时间","水平","环境","我国","模式","结构","效果","措施","设计","质量","基于","基础上","一个","方式","参考","信息","现状","能力","介绍","过程中","建议","体系","选择","10","解决","特征","功能","经济","调查","对策","理论","检测","降低","2011","工程","差异","创新","企业","过程","结论","控制","发生","生产","社会","传统","资源","数据","计算","实践","12","15","价值","文章","20","产业","30","中国","趋势","组织","2010","目标","优化","意义","效率","需求","目的","模型","评价","条件","施工","增加","指标","阐述","程度","发现","地区","领域","11","机制","治疗","国家","运行","学生","教育","医院","为例","进一步","实验","培养","增长","分布","特性","资料","开发","临床","之间","教学","检查","原因","政府","诊断","服务","quot","平均","试验","浅谈","市场","完善","提升","重要的","基础","方案","网络","学习","性能","检验","实施","样本","农业","构建","政策","城市"};
    //private static Set<String> allColumn = new HashSet<>(Arrays.asList("des_c","title_c","keyword_c"));
    public static Set<String> filterSet = new HashSet<>(Arrays.asList(filterWords));

    public static void calcDocNum(Map<String,String> article, Count  wordCount,
                                     Map<Integer,Count> wordCoCount){

            List<String> words = new ArrayList<>();
            allColumn.forEach(col-> words.addAll(Arrays.asList(article.get(col).split(" "))));

            Set<Integer> localWord = Sets.newHashSet();

            words.forEach(word->localWord.add(WordMap.set(word)));

            //更新每个词出现的文档数
            for(Integer word:localWord){
                wordCount.increase(word,1);
            }
            //通过本篇文章中统计情况，更新全局的词语统计和相关词语统计
            for(Integer wordA : localWord){
                Count map = null;
                if(!wordCoCount.containsKey(wordA)) {
                    map = new Count();
                }else{
                    map = wordCoCount.get(wordA);
                }
                //计算机 wordA以下所有wordB出现的次数
                for(Integer wordB : localWord){
                    if(wordA.equals(wordB)){
                        continue;
                    }
                    map.increase(wordB,1);
                }
                wordCoCount.put(wordA,map);
            }
    }

    public static void calcNum(Map<String,String> article, Count  wordCount,
                                  Map<Integer,Count> wordCoCount){

        //List<Integer> words = new ArrayList<>();
        Set<Integer> words = new HashSet<>();
        StringBuffer sb = new StringBuffer();
        allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));

        /*allColumn.forEach(col-> Arrays.asList(article.get(col).split(" "))
                        .forEach(word ->
                                words.add(WordMap.set(word)))
        );*/
        List<Term> terms = ToAnalysis.parse(sb.toString());
        terms.forEach(term->{
            if(term.getNatureStr().contains("n") || term.getNatureStr().contains("v")){
                if(!term.getName().equals(" ") && !filterSet.contains(term.getName())){
                    words.add(WordMap.set(term.getName()));
                }
            }
            /*if(!term.getName().equals(" ")){
                words.add(WordMap.set(term.getName()));
            }*/

            //System.out.println(term);
        });

        //更新每个词出现的文档数
        for(Integer word:words){
            wordCount.increase(word,1);
        }
        //通过本篇文章中统计情况，更新全局的词语统计和相关词语统计
        for(Integer wordA : words){
            Count map = null;
            if(!wordCoCount.containsKey(wordA)) {
                map = new Count();
            }else{
                map = wordCoCount.get(wordA);
            }
            //计算机 wordA以下所有wordB出现的次数
            for(Integer wordB : words){
                if(wordA.equals(wordB)){
                    continue;
                }
                map.increase(wordB,1);
            }
            wordCoCount.put(wordA,map);
        }
        /*for (int i = 0; i < words.size(); i++) {
            Integer wordA = words.get(i);
            Count map = null;
            if (!wordCoCount.containsKey(wordA)) {
                map = new Count();
            } else {
                map = wordCoCount.get(wordA);
            }

            int windowStart = i - HALF_WINDOW < 0 ? 0 : i - HALF_WINDOW;
            int windowEnd = i + HALF_WINDOW > words.size() ? words.size() : i + HALF_WINDOW;
            int windowMid = i;

            //对词窗内的词进行统计 windowWeight
            for (int j = windowStart; j < windowEnd; j++) {
                Integer wordB = words.get(j);
                if (wordA.equals(wordB)) {
                    continue;
                }
                map.increase(wordB, 1);
            }
            wordCoCount.put(wordA,map);
        }*/
    }
    public static Map<Integer,Count> update(Count wordCount, Map<Integer,Count> wordCoCount){
        Map<Integer,Count> res = Maps.newHashMap();

        for(Integer wordA:wordCoCount.keySet()){
            Count p = new Count();
            Count coCount = wordCoCount.get(wordA);
            for(Map.Entry<Integer,Double> entry:coCount.entrySet()){
                Integer wordB = entry.getKey();
                p.set(entry.getKey(), coCount.get(wordB) * 1.0 / wordCount.get(wordA));
            }
            res.put(wordA,p);
        }
        return res;
    }
}
