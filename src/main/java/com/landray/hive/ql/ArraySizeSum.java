package com.landray.hive.ql;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;


/**
 * 求一個升序的数组中，N个最大值的和
 *
 */
public class ArraySizeSum extends UDF {
    /**
     * 返回升序数组的N各最大值的和
     *
     * @param arguments 第一个参数为hive的数组，对应java的list，第二个参数为size
     * @return
     */
    public long evaluate(Object[] arguments) {
        List<Object> list = (List<Object>)arguments[0];
        if(list.size()==0){
            return 0l;
        }
        int index = (int)Double.parseDouble(arguments[1].toString());
        if(list.size()<index){
            index = list.size();
        }
        long result = 0l;
        for (int i = 0; i < index; i++) {
            result += Long.parseLong(list.get(list.size() - 1 - i).toString());
        }
        return result;
    }

    public static void main(String args[]){
        long result = 0l;
        int index =3;
        String[] arr = new String[]{"1","2","3","4"};
        for (int i = 0; i < index; i++) {
            result += Long.parseLong(arr[arr.length - 1 - i]);
        }
        System.out.print(result);

    }
}
