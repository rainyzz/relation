package com.rainyzz.relation.core;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by rainystars on 10/22/2015.
 */
public class WordMap {
    private static Map<String,Integer> wordMap = Maps.newHashMap();
    private static Map<Integer,String> reversedWordMap = Maps.newHashMap();
    public static Integer get(String word){
        return wordMap.get(word);
    }

    public static int size(){
        return wordMap.size();
    }

    public static String get(Integer num){
        return reversedWordMap.get(num);
    }

    public static Integer set(String word){
        if(wordMap.containsKey(word)){
            return get(word);
        }
        int lastNum = wordMap.size();
        wordMap.put(word,lastNum+1);
        reversedWordMap.put(lastNum+1,word);
        return lastNum + 1;
    }
}
