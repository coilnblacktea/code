package com.vdian.bigdata.meta.lineage.service;

import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.lineage.entity.LineageContext;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import com.vdian.bigdata.meta.lineage.enums.SqlSourceEnums;
import com.vdian.bigdata.meta.lineage.exception.UnSupportedException;
import com.vdian.bigdata.meta.lineage.parse.LineParser;
import com.vdian.bigdata.meta.mars.domain.ScheduleDO;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-28  16:53
 **/

@Service
public class LineageSqlParserImpl implements LineageSqlParser {


    private static final Logger logger = LoggerFactory.getLogger(LineageSqlParserImpl.class);

    @Autowired
    private LineParser lineParser;

    @Autowired
    private SqlParseResultProcessor sqlParseResultProcessor;

    @Override
    public List<SQLResult> parseSql(String sql, SqlSourceEnums sqlSourceEnums) {

        checkSqlSource(sqlSourceEnums);
        try {
            return lineParser.parse(sql);
        } catch (Exception e) {
            throw new MetaException(String.format("解析sql[%s]出错，原因是 [%s]", sql, ExceptionUtils.getStackTrace(e)), ErrorCode.SYSTEM_ERROR);
        }


    }

    @Override
    public void batchParseSql(List<ScheduleDO> scheduleDOS, SqlSourceEnums sqlSourceEnums) {
            checkSqlSource(sqlSourceEnums);

            scheduleDOS.stream().forEach(this:: processSql);


    }


    private void checkSqlSource(SqlSourceEnums sqlSourceEnums) {
        if (!sqlSourceEnums.equals(SqlSourceEnums.HIVE)) {
            throw new UnSupportedException("暂时还不支持hive以外的语句进行分析");
        }
    }


    /**
     * 处理脚本
     * @param scheduleDO
     */
    private void processSql(ScheduleDO scheduleDO) {
        LineageContext context = LineageContext.getInstance();

        if(StringUtils.isEmpty(scheduleDO.getScript())){
            context.getParseEmtpyScriptId().add(scheduleDO.getId());
            context.increParseScript();
            return;
        }

        try{
            context.increParseScript();
            List<SQLResult>  sqlParseResults = lineParser.parse(scheduleDO.getScript());
            if(CollectionUtils.isEmpty(sqlParseResults)){
                return ;
            }

            sqlParseResults.stream().forEach(e ->{
                e.setScriptId(scheduleDO.getId());
                e.setJobId(scheduleDO.getJobId());
            });

            //处理结果
            sqlParseResultProcessor.processSqlParseResult(sqlParseResults);
        } catch (Exception e) {
            context.getParseErrorScriptId().add(scheduleDO.getId());
            logger.error("parse script error ,scriptId is [{]],error is [{}}",scheduleDO.getId(),ExceptionUtils.getStackTrace(e));
        }

    }
}

    
