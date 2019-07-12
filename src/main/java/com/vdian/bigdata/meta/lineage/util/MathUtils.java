package com.vdian.bigdata.meta.lineage.util;

import java.text.NumberFormat;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  15:38
 **/


public class MathUtils {


    public static NumberFormat numberFormat = NumberFormat.getInstance();


    static {
        numberFormat.setMaximumFractionDigits(2);
    }

    /**
     * 两个数相除 保留两位小数
     * @param source
     * @param target
     * @return
     */
    public static  double divide(Integer source,Integer target){
        double value = Double.valueOf(source)/target * 100;
        return Double.valueOf(numberFormat.format(value));
    }
}

    
