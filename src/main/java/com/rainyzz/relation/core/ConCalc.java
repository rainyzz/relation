package com.rainyzz.relation.core;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rainystars on 10/23/2015.
 */
public class ConCalc {
    public static void calc(List<Map<String, String>> list, Count  wordCount,
                                     Map<Integer,Count> wordCoCount){
        for(Map<String,String> article:list) {
            //对于每篇文章，获取其摘要
            String abs = article.get("title");
            String keyword = article.get("keyword");
            List<String> keywords = Splitter.on(",").omitEmptyStrings().splitToList(keyword);

            Set<Integer> localWord = Sets.newHashSet();
            List<Term> terms = ToAnalysis.parse(abs + keyword);
            //对于摘要中的文本，统计词语出现次数
            for (Term term : terms) {
                /*if(dict.contains(term.getName()) &&
                        (term.getNatureStr().contains("n") || term.getNatureStr().contains("userDefine"))) {
                    words.add(term.getName());
                }*/
                if(term.getName().length() < 2){
                    continue;
                }
                int index = WordMap.set(term.getName());
                localWord.add(index);
            }
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
    }
    public static Map<Integer,Count>  update(Count wordCount, Map<Integer,Count> wordCoCount){
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
