package com.rainyzz.relation.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;


import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rainystars on 10/26/2015.
 */
public class TransFormer {
    public static final String SQL_FILE_PATH = "C:\\Users\\rainystars\\Desktop\\wanfang_detail.sql";
    public static final String SQL_FILE_OUTPUT_PATH = "D://wanfang.txt";


    static String[] provices = {"北京","天津","上海","重庆","河北","河南","云南","辽宁","黑龙江","湖南","安徽","山东","新疆","江苏","浙江","江西","湖北","广西","甘肃","山西","内蒙","陕西","吉林","福建","贵州","广东","青海 ","西藏","四川","宁夏","海南","台湾","香港","澳门"};

    public static final String SINGLE_OUTPUT_PATH = "D://industry.txt";
    public static final String SPLIT_OUTPUT_PATH = "D:\\industry_split\\";
    public static final String ALL_SPLIT_OUTPUT_PATH = "D:\\all_split\\";
    private static String codeQuery ="code2:T.TB";
    public static void readWfFromSolr(String path){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/wanfang");

        File writename = new File(path);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(writename));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i = 2000; i <= 2013;i++){
            SolrQuery query = new SolrQuery(codeQuery+" AND author_cn:* AND year:"+i);

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
        return tokens;
    }

    public static void main(String[] args){
        //readWfFromSolr(SINGLE_OUTPUT_PATH);
        //readWfSplitterFromSolr(SPLIT_OUTPUT_PATH);
        readAllWfSplitterFromSolr(ALL_SPLIT_OUTPUT_PATH);
    }
}
