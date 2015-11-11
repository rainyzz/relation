package com.rainyzz.relation.core;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.*;

/**
 * Created by rainystars on 10/23/2015.
 */
public class ConCalc {
    private static Set<String> allColumn = new HashSet<>(Arrays.asList("abstract_cn", "title_cn", "keyword_cn"));

    public static final int HALF_WINDOW = 2;

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

        List<Integer> words = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        allColumn.forEach(col-> Arrays.asList(sb.append(article.get(col)).append(" ")));

        /*allColumn.forEach(col-> Arrays.asList(article.get(col).split(" "))
                        .forEach(word ->
                                words.add(WordMap.set(word)))
        );*/
        List<Term> terms = ToAnalysis.parse(sb.toString());
        terms.forEach(term->{
            if(term.getNatureStr().contains("n") || term.getNatureStr().contains("v")){
                if(!term.getName().equals(" ")){
                    words.add(WordMap.set(term.getName()));
                }
            }
            //System.out.println(term);
        });

        //更新每个词出现的文档数
        for(Integer word:words){
            wordCount.increase(word,1);
        }
        //通过本篇文章中统计情况，更新全局的词语统计和相关词语统计
        /*for(Integer wordA : words){
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
        }*/
        for (int i = 0; i < words.size(); i++) {
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
        }
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
