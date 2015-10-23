package com.rainyzz.relation.core;

import com.google.common.collect.Maps;

import java.util.*;

/**
 * Created by rainystars on 10/22/2015.
 */
public class Count {
    private Map<Integer,Double> map = null;
    public Count(){
        map = Maps.newHashMapWithExpectedSize(20);
    }

    public Set<Map.Entry<Integer,Double>> entrySet(){
        return map.entrySet();
    }

    public int size(){
        return map.size();
    }

    public Double get(Integer i){
        return map.get(i);
    }
    public void set(Integer i, Double d){
        map.put(i,d);
    }

    public void increase(Integer index, double num){
        if(map.containsKey(index)){
            map.put(index,map.get(index)+num);
        }else{
            map.put(index,num);
        }
    }

    public List sort(){
        List<Map.Entry> list = new ArrayList(map.entrySet());
        if(list.size() <= 4){
            return list;
        }
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });
        if(list.size() > 40){
            list = list.subList(0,40);
        }
        return list;
    }
}
