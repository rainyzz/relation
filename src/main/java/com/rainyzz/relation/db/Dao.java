package com.rainyzz.relation.db;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by rainystars on 8/3/2015.
 */
public class Dao {
    public static final String DB_NAME = "jdbc:mysql://localhost:3306/wanfang?useUnicode=true&characterEncoding=utf-8";
    public static final String DB_USER = "root";
    public static final String DB_PASSWORD = "root";

    private Database db = new Database(DB_NAME,DB_USER,DB_PASSWORD);

    public List<Map<String,String>> getWanFangByYear(int start,int row,int year){
        return getWanFangBySQL(String.format(
                "SELECT * FROM wanfang_detail WHERE id > %s and id < %s + %s and year = '%s'",start,start,row,year));
    }

    private List<Map<String,String>> getWanFangBySQL(String sql){
        ResultSet rs = db.executeSQL(sql);
        List<Map<String,String>> rst = Lists.newArrayList();
        try {
            while(rs.next()){
                Map<String,String> map = Maps.newHashMap();
                map.put("id",rs.getString("id"));
                map.put("title",rs.getString("title_cn"));
                map.put("abstract",rs.getString("abstract_cn"));
                map.put("keyword",rs.getString("keyword_cn"));
                rst.add(map);
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return rst;
    }

    public List<Map<String,String>> getWanFang(int start,int row){
        return getWanFangBySQL(String.format(
                "SELECT * FROM wanfang_detail WHERE id > %s and id < %s + %s",start,start,row));

    }

    public static void main(String[] args){
        Dao dao = new Dao();
        System.out.println(dao.getWanFang(0,100));
    }
}
