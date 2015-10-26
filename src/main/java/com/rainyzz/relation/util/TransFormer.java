package com.rainyzz.relation.util;

import java.io.*;

/**
 * Created by rainystars on 10/26/2015.
 */
public class TransFormer {
    public static final String SQL_FILE_PATH = "C:\\Users\\rainystars\\Desktop\\wanfang_detail.sql";
    public static final String SQL_FILE_OUTPUT_PATH = "D://wanfang.txt";
    public static void main(String[] args){
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
}
