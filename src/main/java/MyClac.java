import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rainystars on 10/23/2015.
 */
public class MyClac

{
    public static final double E = 2.71828182846;
    public static final double U = 0.1;
    public static final double M = 0.15;
    public static final int HALF_WINDOW = 2;

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
            Set<Integer> local = Sets.newHashSet();

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
                Count count = null;
                if(!wordCoCount.containsKey(wordA)) {
                    count = new Count();
                }else{
                    count = wordCoCount.get(wordA);
                }
                //计算机wordA下所有wordB
                for(int wordB : local){
                    if(wordA == wordB){
                        continue;
                    }
                    count.increase(wordB,1);
                }
                wordCoCount.put(wordA,count);
            }
        }
    }

    public static void update(Map<Integer,Count> frenquecy, Count wordCount, Map<Integer,Count> wordCoCount){

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
}
