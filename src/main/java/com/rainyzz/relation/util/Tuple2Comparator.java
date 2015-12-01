package com.rainyzz.relation.util;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by rainystars on 15/11/27.
 */
public class Tuple2Comparator implements Comparator<Tuple2<String, Double>>,
        Serializable {
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
        String a = o1._1();
        String b = o2._1();
        if(!a.equals(b)){
            return a.compareTo(b);
        }else{
            return Double.compare(o1._2(),o2._2());
        }
    }
}
