package com.vdian.bigdata.meta.lineage.enums;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-08  16:07
 **/


public enum  LineageColTempEnums {

    IS_TEMP(1),
    NOT_TEMP(0);

    private Integer code;

    LineageColTempEnums(Integer code){
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }
}

    
