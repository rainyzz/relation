package com.rainyzz.relation.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Util {



	public static void transKeywordToDic(){
		String keywordFile = "C:\\Users\\rainystars\\Desktop\\final.txt";
		String finalFile = "C:\\Users\\rainystars\\Desktop\\final.dic";
		try {
			FileReader frKeyword = new FileReader(new File(keywordFile));
			BufferedReader br = new BufferedReader(frKeyword);
			String line;
			List<String> keyword = Lists.newArrayListWithExpectedSize(100 * 10000);
			int count = 0;
			while ((line = br.readLine()) != null) {
                if(!Strings.isNullOrEmpty(line)){
                    keyword.add(line.trim());
                }
                System.out.println(count++);
            }

			FileWriter fw = new FileWriter(new File(finalFile));
			for(String word:keyword){
                fw.write(String.format("%s\tuserDefine\t2000\n",word));
            }
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void keywordSplit() throws IOException {
		String fileName = "C:\\Users\\rainystars\\Desktop\\wanfang_detail.txt";
		String fileNamew = "C:\\Users\\rainystars\\Desktop\\keyword.txt";
		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);
		String line;
		Set<String> allKeywords = Sets.newHashSetWithExpectedSize(200000);
		int count = 0;
		while ((line = br.readLine()) != null) {
			if(!Strings.isNullOrEmpty(line)){
				Iterable<String> keywords = Splitter.on(",").omitEmptyStrings().split(line);
				for(String keyword:keywords){
					allKeywords.add(keyword);
				}
			}
			System.out.println(count++);
		}
		FileWriter fw = new FileWriter(new File(fileNamew));
		for(String keyword:allKeywords){
			fw.write(keyword+"\n");
		}
		fw.close();
	}

	public static void mergeBaikeAndKeyword() throws IOException {
		String keywordFile = "C:\\Users\\rainystars\\Desktop\\keyword.txt";
		String baikeFile = "C:\\Users\\rainystars\\Desktop\\baike.txt";
		String finalFile = "C:\\Users\\rainystars\\Desktop\\final.txt";
		FileReader frKeyword = new FileReader(new File(keywordFile));
		FileReader frBaike = new FileReader(new File(baikeFile));
		BufferedReader br = new BufferedReader(frKeyword);
		String line;
		Set<String> keyword = Sets.newHashSetWithExpectedSize(400 * 10000);
		Set<String> finalSet = Sets.newHashSetWithExpectedSize(100 * 10000);
		int count = 0;
		while ((line = br.readLine()) != null) {
			if(!Strings.isNullOrEmpty(line)){
				keyword.add(line.trim());
			}
			System.out.println(count++);
		}
		br = new BufferedReader(frBaike);
		count = 0;
		while ((line = br.readLine()) != null) {
			if(keyword.contains(line.trim())){
				finalSet.add(line.trim());
			}
			System.out.println(count++);
		}
		FileWriter fw = new FileWriter(new File(finalFile));
		for(String word:finalSet){
			fw.write(word+"\n");
		}
		fw.close();
	}

	public static void main(String[] args)  {
		/*try {
			com.rainyzz.relation.util.Util.mergeBaikeAndKeyword();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		Util.transKeywordToDic();

	}

}