package com.rainyzz.relation.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rainystars on 10/26/2015.
 */
public class LineReader {
    final static int SQL_START = 38;
    final static int SQL_END = 4;

    public static Map<String,String> readRecord(String line,int start,int fromEnd){
        Map<String,String> article = new HashMap<String, String>();

        String content = line.substring(start, line.length() - fromEnd);
        String[] data = content.split("', '");
        System.out.println(data);
        if(data.length < 7){
            article.put("title","");
            article.put("abs","");
            article.put("keyword","");
            return article;
        }
        article.put("title",data[0]);
        article.put("abs",data[1]);
        article.put("keyword",data[6]);
        return article;

    }
    public static String[] readRecordToList(String line,int start,int fromEnd){

        String content = line.substring(start, line.length() - fromEnd);
        String[] data = content.split("', '");
        return data;

    }
    public static void main(String args[]){
        String s = "INSERT INTO `wanfang_detail` VALUES ('1', '有害元素对高炉内焦炭热态性能的影响', 'Effect of Harmful Elements on Thermal Properties of Cokes in BF', '针对有害元素在高炉内的反应行为和对焦炭劣化的催化作用,分析了钾、钠、锌、铅、氯化物在高炉内的循环富集过程,重点探讨了其对焦炭热态性能的影响,进一步明确了几种有害元素对焦炭和高炉冶炼的危害性,并根据其循环富集特点提出了控制措施.', '', '张伟,王再义,张立国,王亮,韩子文', ' Zhang Wei , Wang Zaiyi , Zhang Liguo , Wang Liang , Han Ziwen ', '鞍钢股份有限公司技术中心', '鞍钢技术', 'Angang Technology', '2013,(5)', '高炉,有害元素,焦炭', '', 'http://d.wanfangdata.com.cn/Periodical_agjs201305002.aspx', '41.152991112691,122.93622425434');\n";
        String s2 = "有害 元素 高炉 焦炭 性能 影响', '针对 有害 元素 高炉 反应 行为 焦炭 催化 作用 分析 氯化物 高炉 循环 富集 过程 重点 探讨 焦炭 性能 影响 进一步 明确 几种 有害 元素 焦炭 高炉 冶炼 危害性 根据 循环 富集 特点 提出 控制 措施', '张伟,王再义,张立国,王亮,韩子文', '鞍钢股份有限公司技术中心', '鞍钢技术', '2013', '高炉 有害元素 焦炭', '41.152991112691,122.9362242543";
        System.out.println(readRecord(s2,0,0));
        String ss = "信息 动态', '', '', '', '鞍钢技术', '2013', ''  NU";
        String[] data = s2.split("', '");
        System.out.println(data.length);
        for(int i =0; i < data.length; i++){
            System.out.println(i +":" + data[i]);
        }
    }
}
