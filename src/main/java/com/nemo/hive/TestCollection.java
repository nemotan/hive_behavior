package com.nemo.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by nemo on 15/9/24.
 */
public class TestCollection {
    public static void main(String args[]){
        List<Object> ret = new ArrayList<Object>();
        ret.add("12");
        ret.add("43");
        ret.add("34");
        ret.add("21");
        Collections.sort(ret, new Comparator() {
            public int compare(Object o1, Object o2) {
                long v1 = Long.valueOf(o1.toString());
                long v2 = Long.valueOf(o2.toString());
                return v2 > v1 ? -1 : 1;
            }
        });

        if(ret.size()>3){
            ret.subList(0,ret.size()-3).clear();
        }
        System.out.print(ret);
    }
}
