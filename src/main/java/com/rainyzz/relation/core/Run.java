package com.rainyzz.relation.core;

import com.google.common.collect.Maps;
import com.rainyzz.relation.db.Dao;
import com.rainyzz.relation.util.ResWriter;
import org.ansj.library.UserDefineLibrary;
import java.util.*;

public class Run {

    public static final int EACH = 10 * 10000;
    public static final int TOTAL = 1;

    public static final String OUTPUT_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\2010.txt";
    public static final String OUTPUT_DIR = "C:\\Users\\rainystars\\Desktop\\relation\\";
    public static final String LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.dic";

    public static final int START_YEAR = 2012;
    public static final int END_YEAR = 2012;

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
            MyClac.calc(list, frenquecy, wordCount, wordCoCount);
            long endTime=System.currentTimeMillis();
            System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
        }

        ResWriter.writeResult(frenquecy, OUTPUT_PTAH);

    }

    public static void calcPossiblity(){
        Dao dao = new Dao();
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);

        List<Map<String, String>> list;
        Count wordCount = new Count();
        Map<Integer,Count> wordCoCount = Maps.newHashMap();
        long beginTime = System.currentTimeMillis();
        for(int year = START_YEAR; year <= START_YEAR; year++){
            for(int j = 1; j <= 50 ; j++){
                list = dao.getWanFangByYear((j - 1) * EACH, EACH, year);
                ConCalc.calc(list, wordCount, wordCoCount);
                long endTime=System.currentTimeMillis();
                System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
            }
            System.out.println("com.rainyzz.relation.core.WordMap size: " + WordMap.size());
            System.out.println("wordCount size: " + wordCount.size());
            System.out.println("wordCoCount size: " + wordCoCount.size());

            Map<Integer,Count> res = ConCalc.update(wordCount, wordCoCount);
            ResWriter.writeResult(res, OUTPUT_DIR + year + "+contract.txt");
            //writeToSolr(frenquecy,year);
            //清空频率
            wordCount = new Count();
            wordCoCount = Maps.newHashMap();
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
                MyClac.calc(list, frenquecy, wordCount, wordCoCount);
                long endTime=System.currentTimeMillis();
                System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
            }
            System.out.println("com.rainyzz.relation.core.WordMap size: " + WordMap.size());
            System.out.println("frenquency size: " + frenquecy.size());
            System.out.println("wordCount size: " + wordCount.size());
            System.out.println("wordCoCount size: " + wordCoCount.size());

            MyClac.update(frenquecy, wordCount, wordCoCount);
            ResWriter.writeResult(frenquecy, OUTPUT_DIR + year + ".txt");
            //writeToSolr(frenquecy,year);
            //清空频率
            frenquecy = Maps.newHashMapWithExpectedSize(20 * 10000);
            wordCount = new Count();
            wordCoCount = Maps.newHashMap();
        }
    }

    public static void main(String[] args){
        //com.rainyzz.relation.core.Run.calcTotal();
        //com.rainyzz.relation.core.Run.calcByYear();
        Run.calcPossiblity();
    }
}
