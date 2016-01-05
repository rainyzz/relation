package com.rainyzz.relation.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.rainyzz.relation.core.Count;
import com.rainyzz.relation.core.Filter;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;


import java.io.*;
import java.util.*;

/**
 * Created by rainystars on 10/26/2015.
 */
public class TransFormer {
    public static final String SQL_FILE_PATH = "C:\\Users\\rainystars\\Desktop\\wanfang_detail.sql";
    public static final String SQL_FILE_OUTPUT_PATH = "D://wanfang.txt";
    public static final String LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.dic";

    static String[] provices = {/*"北京","天津","上海","重庆","河北","河南","云南","辽宁","黑龙江","湖南","安徽","山东","新疆","江苏","浙江","江西","湖北","广西","甘肃","山西","内蒙","陕西","吉林","福建","贵州","广东","青海","西藏","四川","宁夏","海南","台湾","香港","澳门"*/"青海"};

    public static final String PATH ="";
    public static final String SINGLE_OUTPUT_PATH = "D://s-filter-strict.txt";
    public static final String SPLIT_OUTPUT_PATH = "D:\\industry_split\\";
    public static final String ALL_SPLIT_OUTPUT_PATH = "D:\\qin\\";
    private static String codeQuery ="code1:S";
    public static void readWfFromSolr(String path){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/wanfang");

        File writename = new File(path);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(writename));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i = 2003; i <= 2013;i++){
            SolrQuery query = new SolrQuery(codeQuery+" AND author_cn:* AND year:"+i);

            query.setRows(15 * 10000);

            QueryResponse response = null;
            try {
                response = solr.query(query);
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(response == null){
                continue;
            }
            SolrDocumentList list = response.getResults();
            try {
                for(SolrDocument doc:list){
                    JSONObject jb = new JSONObject(doc);
                    ifToken("title_cn",jb);
                    ifToken("abstract_cn",jb);
                    ifToken("keyword_cn",jb);
                    jb.remove("journal_c");
                    jb.remove("title_c");
                    jb.remove("_version_");

                    boolean flag = false;
                    if(jb.containsKey("workplace")){
                        String wk = jb.get("workplace").toString();

                        for(String pro:provices){
                            if(wk.contains(pro)){
                                jb.put("workplace", pro);
                                flag = true;
                                break;
                            }
                        }

                    }
                    if(flag == false){
                        continue;
                    }

                    out.write(jb + "\n");
                    System.out.println(jb);
                }
                out.flush();
            }catch (IOException e){

            }
        }
        try {
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void GetFilterWords(String path){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/wanfang");
        String[] codes = {"R.RT","N.NT","R.R4","R.R1","R.R2","T.TP","T.TU","N.NP","T.TZ","R.RA","S.ST","T.TQ","N.NA","T.TN","R.R9","R.R6","T.TH","R.R5","T.TA","T.TM","S.S8","R.R3","T.TD","T.TX","T.TS","T.TV","T.TG","R.R16","N.NQ","N.N04","N.N06","T.TB","R.R76","R.R8","S.S2","R.R74","N.N01","T.TE","R.R71","S.S6","R.R73","T.TF","S.S7","T.TJ","T.TK","S.S1","T.TY","S.S5","F.F4","S.SA","C.C91","S.S3","N.N03","R.R75","S.S4","G.GA","S.S9","F.F3","T.TL","G.G4","F.F2","B.BD9","G.G21","G.G25","C.C913","G.GJ","C.CA","G.G3","F.F0","F.FA","C.C97","B.BD","C.CK0"};
        Count<String> finalCount = new Count<>();
        for(int year = 2012; year <= 2012;year++){

            for (String code:codes){
                Count<String> count = new Count<>();


                SolrQuery query = new SolrQuery("code2:"+code+" AND author_cn:* AND year:"+year);
                query.setRows(10 * 10000);

                QueryResponse response = null;
                try {
                    response = solr.query(query);
                } catch (SolrServerException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(response == null){
                    continue;
                }
                SolrDocumentList list = response.getResults();

                for(SolrDocument doc:list) {
                    Set<String> wordSet = new HashSet<>();
                    String s = token(doc.get("title_cn").toString());
                    String s2 = token(doc.get("abstract_cn").toString());
                    wordSet.addAll(Splitter.on(" ").splitToList(s));
                    wordSet.addAll(Splitter.on(" ").splitToList(s2));
                    wordSet.forEach(w->count.increase(w,1));
                }
                List<Map.Entry<String,Double>> top = count.sort(100);
                for(Map.Entry<String,Double> entry:top){
                    finalCount.increase(entry.getKey(),1);
                }
                System.out.println(code+"#"+top);
            }
            List<Map.Entry<String,Double>> finalTop = finalCount.sort(200);

            for(Map.Entry<String,Double> entry:finalTop){
                System.out.println(entry.getKey()+"#"+ entry.getValue()/codes.length);
            }

            System.out.println("total"+codes.length);

        }
    }


    public static void readAllWfSplitterFromSolr(String path){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/wanfang");
        String[] codes = {"R.RT","N.NT","R.R4","R.R1","R.R2","T.TP","T.TU","N.NP","T.TZ","R.RA","S.ST","T.TQ","N.NA","T.TN","R.R9","R.R6","T.TH","R.R5","T.TA","T.TM","S.S8","R.R3","T.TD","T.TX","T.TS","T.TV","T.TG","R.R16","N.NQ","N.N04","N.N06","T.TB","R.R76","R.R8","S.S2","R.R74","N.N01","T.TE","R.R71","S.S6","R.R73","T.TF","S.S7","T.TJ","T.TK","S.S1","T.TY","S.S5","F.F4","S.SA","C.C91","S.S3","N.N03","R.R75","S.S4","G.GA","S.S9","F.F3","T.TL","G.G4","F.F2","B.BD9","G.G21","G.G25","C.C913","G.GJ","C.CA","G.G3","F.F0","F.FA","C.C97","B.BD","C.CK0"};

        for(int year = 2000; year <= 2013;year++){
            Map<String,BufferedWriter> writerMap = new HashMap<>();
            for(String pro :provices){
                BufferedWriter o = null;
                String outName = pro+"#"+year;
                try {
                    o = new BufferedWriter(new FileWriter(new File(path+outName)));
                    writerMap.put(outName,o);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            for (String code:codes){
                SolrQuery query = new SolrQuery("code2:"+code+" AND author_cn:* AND year:"+year);
                query.setRows(10 * 10000);

                QueryResponse response = null;
                try {
                    response = solr.query(query);
                } catch (SolrServerException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(response == null){
                    continue;
                }
                SolrDocumentList list = response.getResults();
                if(list.size() < 2000){
                    continue;
                }
                try {
                    for(SolrDocument doc:list){
                        JSONObject jb = new JSONObject(doc);
                        ifToken("title_cn",jb);
                        ifToken("abstract_cn",jb);
                        ifToken("keyword_cn",jb);
                        jb.remove("journal_c");
                        jb.remove("title_c");
                        jb.remove("_version_");

                        boolean flag = false;
                        BufferedWriter o = null;
                        if(jb.containsKey("workplace")){
                            String wk = jb.get("workplace").toString();

                            for(String pro:provices){
                                if(wk.contains(pro)){
                                    jb.put("workplace", pro);
                                    flag = true;
                                    o =writerMap.get(pro+"#"+year);
                                    break;
                                }
                            }

                        }
                        if(flag == false){
                            continue;
                        }
                        if (o != null){
                            o.write(jb + "\n");
                        }
                        System.out.println(jb);
                    }

                }catch (IOException e){

                }
            }
            try {
                for(String key:writerMap.keySet()){
                    BufferedWriter o = writerMap.get(key);
                    o.flush();
                    o.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
    public static void readWfSplitterFromSolr(String path){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/wanfang");

        for(int i = 2000; i <= 2013;i++){
            SolrQuery query = new SolrQuery(codeQuery+" AND author_cn:* AND year:"+i);
            Map<String,BufferedWriter> writerMap = new HashMap<>();
            for(String pro :provices){
                BufferedWriter o = null;
                String outName = pro+"#"+i;
                try {
                    o = new BufferedWriter(new FileWriter(new File(path+outName)));
                    writerMap.put(outName,o);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            query.setRows(10 * 10000);

            QueryResponse response = null;
            try {
                response = solr.query(query);
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(response == null){
                continue;
            }
            SolrDocumentList list = response.getResults();
            try {
                for(SolrDocument doc:list){
                    JSONObject jb = new JSONObject(doc);
                    ifToken("title_cn",jb);
                    ifToken("abstract_cn",jb);
                    ifToken("keyword_cn",jb);
                    jb.remove("journal_c");
                    jb.remove("title_c");
                    jb.remove("_version_");

                    boolean flag = false;
                    BufferedWriter o = null;
                    if(jb.containsKey("workplace")){
                        String wk = jb.get("workplace").toString();

                        for(String pro:provices){
                            if(wk.contains(pro)){
                                jb.put("workplace", pro);
                                flag = true;
                                o =writerMap.get(pro+"#"+i);
                                break;
                            }
                        }

                    }
                    if(flag == false){
                        continue;
                    }
                    if (o != null){
                        o.write(jb + "\n");
                    }
                    System.out.println(jb);
                }
                for(String key:writerMap.keySet()){
                    BufferedWriter o = writerMap.get(key);
                    o.flush();
                    o.close();
                }
            }catch (IOException e){

            }
        }
    }
    public static void readFromSolr(String path){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/dev");

        File writename = new File(path);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(writename));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i = 1915; i <= 2015;i++){
            SolrQuery query = new SolrQuery("A101_year_s:"+i);
            query.setRows(10 * 10000);

            QueryResponse response = null;
            try {
                response = solr.query(query);
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(response == null){
                continue;
            }
            SolrDocumentList list = response.getResults();
            try {
                for(SolrDocument doc:list){
                    JSONObject jb = new JSONObject(doc);
                    ifToken("title_c",jb);
                    ifToken("des_c",jb);
                    ifToken("keyword_c",jb);

                    out.write(jb + "\n");
                    System.out.println(jb);
                }
                out.flush();
            }catch (IOException e){

            }
        }
        try {
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void ifToken(String col,JSONObject jb){
        if(jb.containsKey(col)){
            jb.put(col,token(jb.get(col).toString()));
        }else{
            jb.put(col,"");
        }
    }

    private static String token(String s){


        List<Term> terms = ToAnalysis.parse(s);
        List<String> words = Lists.newArrayList();

        for(Term term:terms) {
            if (term.getNatureStr().contains("n") || term.getNatureStr().contains("v") || term.getNatureStr().contains("user")) {

            }else{
                continue;
            }

            String word = term.getName();

            if(!Filter.dict.contains(word)){
                continue;
            }
            if(filterSet.contains(word)){
                continue;
            }
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
        return tokens;
    }
    public static final String[] filterWords ={"主要","进行","研究","通过","具有","分析","以及","方法","影响","提高","应用","重要","结果","有效","作用","不同","同时","采用","技术","提供","情况","基础","过程","探讨","发展","问题","结合","利用","方面","作为","提出","可以","相关","系统","因素","一种","工作","存在","质量","变化","效果","建立","结构","表明","本文","时间","环境","实现","水平","分别","特点","比较","设计","明显","降低","基于","使用","条件","管理","针对","介绍","关系","试验","随着","控制","处理","显著","模型","要求","增加","我国","措施","根据","其中","检测","发生","建设","差异","一个","生产","意义","理论","中国","经济","功能","实验","性能","方式","计算","解决","出现","特征","对照","得到","结论","评价","目的","发现","反应","实际","治疗","工程","能力","参数","达到","综合","高于","优化","企业","含量","诊断","现状","临床","资料","目前","地区","观察","社会","平均","疾病","促进","组织","疗效","患者","选择","特性","分为","程度","成为","及其","统计学","体系","机制","随机","两组","一定","实践","手术","表现","模式","方案","资源","产生","减少","设备","回顾","为了","测定","运行","需要","显示","阐述","并发症","分布","药物","模拟","最后","如何","我院","检查","材料","加强","蛋白","之间","工艺","细胞","健康","生长","产业","信息","温度","症状","指标","创新","原因","正常","感染","验证","软件","对策","运用","改善","教学","医院","可能","传统","建议","算法","表达","生态","年龄","推广","仿真","严重","一些","市场","浓度"};
    //private static Set<String> allColumn = new HashSet<>(Arrays.asList("des_c","title_c","keyword_c"));
    public static Set<String> filterSet = new HashSet<>(Arrays.asList(filterWords));

    public static void main(String[] args){
        Filter.buildDict();
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);
        //readWfFromSolr(SINGLE_OUTPUT_PATH);
        //readWfSplitterFromSolr(SPLIT_OUTPUT_PATH);
        readAllWfSplitterFromSolr(ALL_SPLIT_OUTPUT_PATH);
        //GetFilterWords(PATH);
    }
}
