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
import java.util.List;

/**
 * Created by rainystars on 10/26/2015.
 */
public class TransFormer {
    public static final String SQL_FILE_PATH = "C:\\Users\\rainystars\\Desktop\\wanfang_detail.sql";
    public static final String SQL_FILE_OUTPUT_PATH = "D://wanfang.txt";
    public static final String OUTPUT_PATH = "D://standard-all.txt";

    static String[] provices = {"北京","天津","上海","重庆","河北","河南","云南","辽宁","黑龙江","湖南","安徽","山东","新疆","江苏","浙江","江西","湖北","广西","甘肃","山西","内蒙","陕西","吉林","福建","贵州","广东","青海 ","西藏","四川","宁夏","海南","台湾","香港","澳门"};

    public static final String WF_OUTPUT_PATH = "D://wf.txt";
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
            SolrQuery query = new SolrQuery("journal_c:医学 AND author_cn:* AND year:"+i);
            query.setRows(2 * 10000);

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
        //readFromSolr(OUTPUT_PATH);
        readWfFromSolr(WF_OUTPUT_PATH);
    }
}
