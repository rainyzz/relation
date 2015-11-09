package com.rainyzz.relation.core;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.rainyzz.relation.db.Dao;
import com.rainyzz.relation.util.LineReader;
import com.rainyzz.relation.util.ResWriter;
import org.ansj.library.UserDefineLibrary;

import java.io.*;
import java.util.*;

public class Run {

    public static final int EACH = 10 * 10000;
    public static final int TOTAL = 3;

    public static final String OUTPUT_DIR = "C:\\Users\\rainystars\\Desktop\\relation\\";
    public static final String LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.dic";

    public static final int START_YEAR = 2012;
    public static final int END_YEAR = 2013;

    public static final String INPUT_PTAH = "D:\\spark\\wf-computer.txt";
    public static final String OUTPUT_PTAH = "D:\\spark\\output-num.txt";

    public static void calcPossiblity(){
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);
        long beginTime = System.currentTimeMillis();

        Count wordCount = new Count();
        Map<Integer,Count> wordCoCount = Maps.newHashMap();

        try {
            FileReader fr = new FileReader(new File(INPUT_PTAH));
            BufferedReader br = new BufferedReader(fr);
            FileWriter fw = new FileWriter(new File(OUTPUT_PTAH));
            String line;
            while ((line = br.readLine()) != null) {
                Map<String,String> article = LineReader.readRecord(line);
                ConCalc.calcNum(article, wordCount, wordCoCount);
                long endTime=System.currentTimeMillis();
                System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
            }
            fw.flush();
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<Integer,Count> res = ConCalc.update(wordCount, wordCoCount);
        ResWriter.writeResult(res, OUTPUT_PTAH);
        //writeToSolr(frenquecy,year);

    }

    public static void calcByYear(){
        Dao dao = new Dao();
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,LIBRARY_PTAH);

        long beginTime = System.currentTimeMillis();
        for(int year = START_YEAR; year <= END_YEAR; year++){
            List<Map<String, String>> list;
            Map<Integer,Count> frenquecy = Maps.newHashMapWithExpectedSize(5 * 10000);
            Count wordCount = new Count();
            Count wordTotalCount = new Count();
            Map<Integer,Count> wordCoCount = Maps.newHashMap();

            for(int j = 1; j <= TOTAL * 10 ; j++){
                list = dao.getWanFangByYear((j - 1) * EACH, EACH, year);
                MyClac.calc(list, wordTotalCount, frenquecy, wordCount, wordCoCount);
                long endTime=System.currentTimeMillis();
                System.out.println("Calculated in " + (endTime - beginTime) / 1000.0 + "s");
            }
            System.out.println("WordMap size: " + WordMap.size());
            System.out.println("frenquency size: " + frenquecy.size());
            System.out.println("wordCount size: " + wordCount.size());
            System.out.println("wordCoCount size: " + wordCoCount.size());

            MyClac.update(frenquecy,wordTotalCount,wordCount, wordCoCount);
            ResWriter.writeResult(frenquecy, OUTPUT_DIR + year + ".txt");
            //writeToSolr(frenquecy,year);
        }
    }

    public static void main(String[] args){
        //calcByYear();
        calcPossiblity();
    }
}
