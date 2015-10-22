import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.*;
import java.util.*;


public class ProcessRaw {

    public static final double E = 2.71828182846;
    public static final double U = 0.1;
    public static final double M = 0.15;
    public static final int EACH = 10 * 10000;
    public static final int TOTAL = 1;
    public static final int HALF_WINDOW = 2;
    public static final String OUTPUT_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\2010.txt";
    public static final String OUTPUT_DIR = "C:\\Users\\rainystars\\Desktop\\relation\\";
    public static final String LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.dic";
    public static final String BUILD_LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.txt";

    public static final int START_YEAR = 2012;
    public static final int END_YEAR = 2012;

    public static Set<String> dict = Sets.newHashSetWithExpectedSize(400 * 10000);

    public static void buildDict(){
        long beginTime = System.currentTimeMillis();

        String fileName = BUILD_LIBRARY_PTAH;
        try {
            FileReader fr = new FileReader(new File(fileName));
            BufferedReader br = new BufferedReader(fr);
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if(line.trim().length() < 2){
                    continue;
                }
                if(!Strings.isNullOrEmpty(line)){
                    dict.add(line.trim());
                }
                count++;
                if(count % 100000 == 0){
                    System.out.println(count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime=System.currentTimeMillis();
        long costTime = (endTime - beginTime);
        System.out.println("Dict built in " + costTime / 1000.0 + "s");
    }



    public static void calc(List<Map<String, String>> list, Map<Integer,Count> frenquecy,Count wordCount,
                            Map<Integer,Count> wordCoCount){
        for(Map<String,String> article:list){
            //对于每篇文章，获取其摘要
            String abs = article.get("title");
            String keyword = article.get("keyword");
            List<String> keywords = Splitter.on(",").omitEmptyStrings().splitToList(keyword);

            List<Term> terms = ToAnalysis.parse(abs);
            //将分词中不是keyword的词语去掉
            List<String> words = Lists.newArrayList();
            for(Term term:terms) {
                /*if(dict.contains(term.getName()) &&
                        (term.getNatureStr().contains("n") || term.getNatureStr().contains("userDefine"))) {
                    words.add(term.getName());
                }*/
                if(term.getName().length() < 2){
                    continue;
                }
                words.add(term.getName());
            }
            Set<Integer> local =Sets.newHashSet();

            for(int i = 0; i < words.size(); i++){

                String word = words.get(i);

                int curWordIndex = WordMap.set(word);
                //计算每个词出现的次数

                local.add(curWordIndex);

                //划分词窗
                int windowStart = i - HALF_WINDOW < 0 ? 0 : i - HALF_WINDOW;
                int windowEnd = i + HALF_WINDOW > words.size() ? words.size(): i + HALF_WINDOW;
                int windowMid = i;
                List<String> wordWindow = words.subList(windowStart, windowEnd);

                Count count = null;
                if(!frenquecy.containsKey(curWordIndex)) {
                    count = new Count();
                }else{
                    count = frenquecy.get(curWordIndex);
                }

                //对词窗内的词进行统计 coCount
                int start  = windowStart;
                for(String windowWord:wordWindow){
                    if(word.equals(windowWord)){
                        continue;
                    }
                    // 根据距离，计算相似度
                    int distance = Math.abs(i-start);
                    double weight = U * Math.pow(E,- U * distance);

                    int windowWordIndex = WordMap.set(windowWord);
                    count.increase(windowWordIndex,weight);
                }

                //对于当前词，其与所有其他字段中词语都有关系。 diffCoCount
                for(String key:keywords){
                    if(word.equals(key)){
                        continue;
                    }
                    int keywordIndex = WordMap.set(key);
                    count.increase(keywordIndex,M);

                }
                frenquecy.put(curWordIndex,count);
            }
            //计算机 F(w)
            for(int index : local){
                wordCount.increase(index,1);
            }
            //计算词语直接共同出现的次数 F(w1, w2)
            for(int wordA : local){
                for(int wordB : local){
                    if(wordA == wordB){
                        continue;
                    }
                    Count count = null;
                    if(!wordCoCount.containsKey(wordA)) {
                        count = new Count();
                    }else{
                        count = wordCoCount.get(wordA);
                    }
                    count.increase(wordB,1);
                    wordCoCount.put(wordA,count);
                }
            }
        }
    }


    public static void calcTotal(){

        Dao dao = new Dao();
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);

        List<Map<String, String>> list;
        Map<Integer,Count> frenquecy = Maps.newHashMapWithExpectedSize(20 * 10000);
        Count wordCount = new Count();
        Map<Integer,Count> wordCoCount = Maps.newHashMap();
        long beginTime = System.currentTimeMillis();

        for(int j = 1; j <= TOTAL; j++){
            list = dao.getWanFang((j - 1) * EACH, EACH);
            calc(list,frenquecy,wordCount,wordCoCount);
            long endTime=System.currentTimeMillis();
            System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
        }

        writeResult(frenquecy,OUTPUT_PTAH);

    }

    private static void calcContract(List<Map<String, String>> list, Map<String,Integer>  wordCount,
                                     Map<String,Map<String,Integer>> wordCoCount){
        for(Map<String,String> article:list) {
            //对于每篇文章，获取其摘要
            String abs = article.get("abstract");
            String keyword = article.get("keyword");
            List<String> keywords = Splitter.on(",").omitEmptyStrings().splitToList(keyword);
            Map<String,Integer> localCount = Maps.newHashMap();
            List<Term> terms = ToAnalysis.parse(abs);
            //对于摘要中的文本，统计词语出现次数
            for (Term term : terms) {
                /*if(dict.contains(term.getName()) &&
                        (term.getNatureStr().contains("n") || term.getNatureStr().contains("userDefine"))) {
                    words.add(term.getName());
                }*/
                if(localCount.containsKey(term.getName())){
                    localCount.put(term.getName(),localCount.get(term.getName()) + 1);
                }else{
                    localCount.put(term.getName(), 1);
                }
            }
            for(String word:localCount.keySet()){
                if(wordCount.containsKey(word)){
                    wordCount.put(word,wordCount.get(word) + 1);
                }else{
                    wordCount.put(word,1);
                }
            }
            //通过本篇文章中统计情况，更新全局的词语统计和相关词语统计
            Set<String> set = (localCount.keySet());
            for(String wordA : set){
                for(String wordB : set){
                    if(wordA.equals(wordB)){
                        continue;
                    }
                    Map<String,Integer> map = null;
                    if(!wordCoCount.containsKey(wordA)) {
                        map = Maps.newHashMapWithExpectedSize(20);
                    }else{
                        map = wordCoCount.get(wordA);
                    }

                    if(map.containsKey(wordB)){
                        map.put(wordB,map.get(wordB) + 1);
                    }else{
                        map.put(wordB, 1);
                    }
                    wordCoCount.put(wordA,map);
                }
            }

        }
    }
    private static Map<String,Map<String,Double>>  calcContractResult(Map<String,Integer>  wordCount,
             Map<String,Map<String,Integer>> wordCoCount){
        Map<String,Map<String,Double>> res = Maps.newHashMap();

        for(String word:wordCoCount.keySet()){
            Map<String,Double> p = Maps.newHashMap();
            Map<String,Integer> coCount = wordCoCount.get(word);
            for(String wordB:coCount.keySet()){
                p.put(wordB, coCount.get(wordB) * 1.0 / wordCount.get(wordB));
            }
            res.put(word,p);
        }

        return res;

    }

    public static void calcPossiblity(){
        Dao dao = new Dao();
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);

        List<Map<String, String>> list;
        Map<String,Integer> wordCount = Maps.newHashMapWithExpectedSize(20 * 10000);
        Map<String,Map<String,Integer>> wordCoCount = Maps.newHashMapWithExpectedSize(20 * 10000);
        long beginTime = System.currentTimeMillis();
        for(int year = START_YEAR; year <= START_YEAR; year++){
            for(int j = 1; j <= 2 ; j++){
                list = dao.getWanFangByYear((j - 1) * EACH, EACH, year);
                calcContract(list, wordCount, wordCoCount);
                long endTime=System.currentTimeMillis();
                System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
            }
            Map<String,Map<String,Double>> res = calcContractResult(wordCount,wordCoCount);
            //writeResult(res,OUTPUT_DIR + year + "+contract.txt");
            //writeToSolr(frenquecy,year);
            //清空频率
            wordCount = Maps.newHashMapWithExpectedSize(20 * 10000);
            wordCoCount = Maps.newHashMapWithExpectedSize(20 * 10000);
        }
    }
    public static void updateSim(Map<Integer,Count> frenquecy, Count wordCount, Map<Integer,Count> wordCoCount){

        Set<Integer> set = new HashSet(frenquecy.keySet());
        for(int wordA:set){
            Count count = frenquecy.get(wordA);
            Count newCount = new Count();
            for(Map.Entry<Integer,Double> entry : count.entrySet()){
                int wordB = entry.getKey();
                double value = 0;
                if(wordCoCount.get(wordA) == null || wordCount.get(wordA) == null || wordCoCount.get(wordA).get(wordB) == null){
                    value = 0;
                }else{
                    value = entry.getValue() / wordCount.get(wordA)
                            * Math.log10(wordCoCount.get(wordA).get(wordB) / wordCount.get(wordA) + 1);
                }
                newCount.set(wordB,value);
             }
            frenquecy.put(wordA,newCount);
        }
    }

    public static void calcByYear(){

        Dao dao = new Dao();
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);

        List<Map<String, String>> list;
        Map<Integer,Count> frenquecy = Maps.newHashMapWithExpectedSize(20 * 10000);
        Count wordCount = new Count();
        Map<Integer,Count> wordCoCount = Maps.newHashMap();
        long beginTime = System.currentTimeMillis();
        for(int year = START_YEAR; year <= END_YEAR; year++){
            for(int j = 1; j <= TOTAL * 10 ; j++){
                list = dao.getWanFangByYear((j - 1) * EACH, EACH, year);
                calc(list,frenquecy,wordCount,wordCoCount);
                long endTime=System.currentTimeMillis();
                System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
            }
            System.out.println("WordMap size: " + WordMap.size());
            System.out.println("frenquency size: " + frenquecy.size());
            System.out.println("wordCount size: " + wordCount.size());
            System.out.println("wordCoCount size: " + wordCoCount.size());


            updateSim(frenquecy, wordCount, wordCoCount);
            writeResult(frenquecy,OUTPUT_DIR + year + ".txt");
            //writeToSolr(frenquecy,year);
            //清空频率
            frenquecy = Maps.newHashMapWithExpectedSize(20 * 10000);
        }
    }
    public static void printResult(Map<String,Map<String,Double>> frenquecy){
        for(String key:frenquecy.keySet()){
            Map<String,Double> map = frenquecy.get(key);
            List list = new ArrayList(map.entrySet());
            if(list.size() <= 4){
                continue;
            }
            Collections.sort(list, new Comparator() {
                public int compare(Object o1, Object o2) {
                    return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                            .compareTo(((Map.Entry) (o2)).getValue());
                }});
            System.out.println(key + ": " + list);
        }
    }
    public static void writeToSolr(Map<String,Map<String,Double>> frenquecy,int year){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/relation");
        int count = 0;
        for(String key:frenquecy.keySet()){

            Map<String,Double> map = frenquecy.get(key);
            List list = new ArrayList(map.entrySet());
            if(list.size() <= 4){
                continue;
            }
            Collections.sort(list, new Comparator() {
                public int compare(Object o1, Object o2) {
                    return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                            .compareTo(((Map.Entry) (o2)).getValue());
                }});
            if(list.size() > 50){
                list = list.subList(0,45);
            }
            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.addField("id", "NEW");
            solrDoc.addField("word_s", key);
            solrDoc.addField("relation_s", list.toString());
            solrDoc.addField("year_s", year);


            count++;
            try {
                solr.add(solrDoc);
                if(count %1000 == 0){
                    solr.commit();
                }
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static List<String>  trans(List<Map.Entry> list){
        List<String> res = Lists.newArrayList();
        for(Map.Entry<Integer,Double> entry:list){
            res.add(WordMap.get(entry.getKey()) + ":" + entry.getValue());
        }
        return res;
    }


    public static void writeResult(Map<Integer,Count> frenquecy,String filename){

        try {
            FileWriter fw = new FileWriter(new File(filename));
            for(Integer index:frenquecy.keySet()){

                Count map = frenquecy.get(index);
                List<Map.Entry> list = map.sort();
                fw.write(WordMap.get(index) + ": " + trans(list) +"\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args){
        ProcessRaw.buildDict();
        //ProcessRaw.calcTotal();
        ProcessRaw.calcByYear();
        //ProcessRaw.calcPossiblity();
    }
}
