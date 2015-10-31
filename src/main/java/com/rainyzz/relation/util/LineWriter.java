package com.rainyzz.relation.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.List;
import java.util.Map;

/**
 * Created by rainystars on 10/26/2015.
 */
public class LineWriter {

    private static int indexed[] = {0,1};
    private static int comma[] = {6};

    public static String token(String[] article){

        //将指定字段分词
        for(int index:indexed){
            List<Term> terms = ToAnalysis.parse(article[index]);
            List<String> words = Lists.newArrayList();

            for(Term term:terms) {
                String word = term.getName();
                if(word.length() < 2){
                    continue;
                }
                char w = word.charAt(0);
                if(('a' <= w && w <= 'z') || ('A' <= w && w <= 'Z')  || Character.isDigit(w)){
                    continue;
                }
                if(w == '#' || w == '\'' || w == '%' || w== '@' || w=='&' || w =='—'){
                    continue;
                }
                words.add(term.getName());
            }

            String tokens = Joiner.on(" ").skipNulls().join(words);
            article[index] = tokens;
        }
        //将指定字段按符号进行分割
        for(int index:comma){
            String s = article[index];
            String tokens = Joiner.on(" ").skipNulls().join(s.split(","));
            article[index] = tokens;
        }

        return Joiner.on("', '").skipNulls().join(article);
    }
    public static void main(String[] args){
        String s = "INSERT INTO `wanfang_detail` VALUES ('有害元素对高炉内焦炭热态性能的影响', '针对有害元素在高炉内的反应行为和对焦炭劣化的催化作用,分析了钾、钠、锌、铅、氯化物在高炉内的循环富集过程,重点探讨了其对焦炭热态性能的影响,进一步明确了几种有害元素对焦炭和高炉冶炼的危害性,并根据其循环富集特点提出了控制措施.', '张伟,王再义,张立国,王亮,韩子文', '鞍钢股份有限公司技术中心', '鞍钢技术', '2013', '高炉,有害元素,焦炭', '41.152991112691,122.93622425434');";

        //System.out.println(LineWriter.token(s));
        System.out.println(LineWriter.token(LineReader.readRecordToList(s,LineReader.SQL_START,LineReader.SQL_END)));


    }
    
}
