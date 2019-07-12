package com.vdian.bigdata.meta.lineage.service;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  17:27
 **/


public interface LineageAnalysisService {


    /**
     * 全量构建 支持 输入时间范围
     * @param interval
     */
    void buildAll(Integer start);


    /**
     * 每天定时调度执行 前一天执行成功的sql
     */
    void autoParse();
}
