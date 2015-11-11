package com.rainyzz.relation.util;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import javax.lang.model.element.Element;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rainystars on 11/11/2015.
 */
public class Spider {
    private static final String BASE_URL = "http://c.g.wanfangdata.com.cn/PeriodicalSubject.aspx?NodeId=";
    public static void getPage(){
        Document doc = null;
        try {
            doc = Jsoup.connect("http://c.g.wanfangdata.com.cn/").timeout(10000).get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String,List> all = new HashMap<>();
        Elements el = doc.select("table");
        el.forEach(e ->{
           String firstClass = e.select("th t").text();
            String url = e.select("th a").attr("href").split("=")[1];
            List<String> list = new ArrayList<>();
           Elements subs = e.getElementsByTag("li");
            subs.forEach(s->{
                list.add(s.select("t").text()+"#"+s.select("a").attr("href").split("=")[1]);
            });
            all.put(firstClass+"#"+url,list);
        });

        /*System.out.println(all);
        for(String key:all.keySet()){
            System.out.println(key.split("#")[0] + "\t" + key.split("#")[1]);
            List<String> subs =all.get(key);
            for(String s:subs){
                System.out.println(s.split("#")[0]+"\t"+s.split("#")[1]);
            }
        }*/

        File writename = new File("D://j.txt");
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(writename));
            Map<String,List<String>> finals = getAll(all);
            System.out.println(finals);
            for(String key:finals.keySet()){
                List<String> list = finals.get(key);
                for(String j:list){
                    out.write(j+"\t"+key.split("\\.")[0]+"\t"+key+"\n");
                }
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Map<String,List<String>> getAll(Map<String,List> all){
        Document doc = null;
        Map<String,List<String>> finals = new HashMap<>();
        for(String key: all.keySet()){
            List<String> list = all.get(key);
            for(String second:list){
                String code = second.split("#")[1];
                String url = BASE_URL + code;

                try {
                    doc = Jsoup.connect(url).timeout(10000).get();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String endPage = "1";
                if(doc.select("td.record_end a").last() != null){
                    endPage = doc.select("td.record_end a").last().attr("href").split("PageNo=")[1];
                }
                int end = Integer.parseInt(endPage);
                List<String> codeAll = new ArrayList<>();
                for(int i = 1; i<= end;i++){
                    try {
                        doc = Jsoup.connect(url+"&PageNo="+i).timeout(10000).get();
                    } catch (IOException e) {
                        i--;
                        System.out.println("retry");
                        continue;

                    }
                    Elements journals = doc.select("ul.record_items11 li");

                    journals.forEach(j->{
                        codeAll.add(j.text());
                        System.out.println(j.text());
                    });
                }
                finals.put(code,codeAll);
            }
        }
        return finals;
    }

    public static void main(String[] args){
        getPage();
    }

}
