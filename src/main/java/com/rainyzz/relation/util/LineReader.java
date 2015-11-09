package com.rainyzz.relation.util;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rainystars on 10/26/2015.
 */
public class LineReader {

    public static Map<String,String> readRecord(String line){
        Map<String,String> article = JSON.parseObject(line,Map.class);
        System.out.println(article);
        return  article;
    }

    public static void main(String args[]){
        readRecord("{\"A001\":\"8021418\",\"A100\":\"GB 51139-2015\",\"A104\":\"CN-GB\",\"A885\":\"CN\",\"_version_\":1516988881709301760,\"id\":\"8021418\",\"title_c\":\"纤维素 纤维 工厂 设计 规范\"}");
    }
}
