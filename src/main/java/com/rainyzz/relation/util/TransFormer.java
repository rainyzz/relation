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
    public static final String OUTPUT_PATH = "D://standard.txt";

    public static void readFromSolr(){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/dev");
        SolrQuery query = new SolrQuery("A104:CN-GB");
        query.setRows(60000);

        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(response == null){
            return;
        }
        SolrDocumentList list = response.getResults();
        File writename = new File(OUTPUT_PATH);
        BufferedWriter out = null;

        try {
            out = new BufferedWriter(new FileWriter(writename));

            for(SolrDocument doc:list){

                JSONObject jb = new JSONObject(doc);
                if(jb.containsKey("title_c")){
                    jb.put("title_c",token(jb.get("title_c").toString()));
                }
                if(jb.containsKey("des_c")){
                    jb.put("des_c",token(jb.get("des_c").toString()));
                }
                if(jb.containsKey("keyword_c")){
                    jb.put("keyword_c",token(jb.get("keyword_c").toString()));
                }
                out.write(jb + "\n");
                System.out.println(jb);
                //break;
            }
            out.flush();
            out.close();
        }catch (IOException e){

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

    public void transform(){
        File file = new File(SQL_FILE_PATH);
        BufferedReader reader = null;
        File writename = new File(SQL_FILE_OUTPUT_PATH);
        BufferedWriter out = null;

        try {
            out = new BufferedWriter(new FileWriter(writename));
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            int count = 1;

            while ((line = reader.readLine()) != null) {
                String newLine = LineWriter.token(LineReader.readRecordToList(line,LineReader.SQL_START,LineReader.SQL_END));
                out.write(newLine+"\n");
                System.out.println("line " + count);
                if(count % 10000 == 0){
                    out.flush();
                }
                count++;
            }
            out.flush();
            out.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }


    public static void main(String[] args){
        readFromSolr();
    }
}
