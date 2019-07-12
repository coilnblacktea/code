package com.vdian.bigdata.meta.lineage.service;

import com.vdian.bigdata.meta.lineage.entity.LineageContext;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import com.vdian.bigdata.meta.lineage.enums.SqlSourceEnums;
import com.vdian.bigdata.meta.mars.domain.ScheduleDO;

import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-28  16:53
 **/


public interface LineageSqlParser {


    /**
     * 接口为了调试使用
     * @param sql
     * @param sqlSourceEnums
     * @return
     */
    List<SQLResult>  parseSql(String sql, SqlSourceEnums sqlSourceEnums);


    /**
     * 批量处理 分页拿到的调度脚本
     * @param scheduleDOS
     * @param sqlSourceEnums
     */
    void batchParseSql(List<ScheduleDO> scheduleDOS, SqlSourceEnums sqlSourceEnums);
}
