package com.vdian.bigdata.meta.lineage.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.vdian.bigdata.meta.bo.ResourceColumnBO;
import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.lineage.cache.LineageCacheService;
import com.vdian.bigdata.meta.lineage.entity.ColLine;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import com.vdian.bigdata.meta.lineage.util.ColExtractUtil;
import com.vdian.bigdata.meta.lineage.util.SqlResultUtil;
import com.vdian.bigdata.meta.meta.domain.lineage.LineageFeature;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-04  10:45
 **/

@Service
public class SqlParseResultProcessor {


    private static final Logger logger = LoggerFactory.getLogger(SqlParseResultProcessor.class);

    @Autowired
    private ColExtractUtil colExtractUtil;


    @Autowired
    private SqlResultUtil sqlResultUtil;


    @Autowired
    private MetaColLineageMapper lineageMapper;

    @Autowired
    private LineageCacheService cacheService;


    private Set<String> temporyTables = Sets.newHashSet();



    @Transactional(transactionManager = "metaTransactionManager",rollbackFor = Exception.class)
    public void processSqlParseResult(List<SQLResult> sqlParseResults){

       temporyTables =  sqlResultUtil.identifyTemporyTables(sqlParseResults);

       List<MetaColLineageDO> colLineageDOS = Lists.newArrayList();
       sqlParseResults.stream().map(this::trans).forEach(colLineageDOS ::addAll);


       if(CollectionUtils.isEmpty(colLineageDOS)){
            return;
       }

       lineageMapper.batchDelete(colLineageDOS.stream().map(MetaColLineageDO::getColResourceId).collect(Collectors.toList()));

       int inserts = lineageMapper.batchInsert(colLineageDOS);
       if(colLineageDOS.size() > inserts){
           throw new MetaException(String.format("一共[%s]条需要插入的记录,插入了[%s}条记录，没有完全插入，回滚!",colLineageDOS.size(),inserts), ErrorCode.SYSTEM_ERROR);
       }
    }


    /**
     *
     * @param sqlResult
     * @return
     */
    private List<MetaColLineageDO> trans(SQLResult sqlResult){
        if(CollectionUtils.isEmpty(sqlResult.getOutputTables())){
            return Lists.newArrayList();
        }

        if(sqlResult.getOutputTables().size() > 1){
            logger.error("[{}} too many outputtables, error ",sqlResult);
            return Lists.newArrayList();
        }

        if(CollectionUtils.isEmpty(sqlResult.getColLineList())){
            return Lists.newArrayList();
        }


        return sqlResult.getColLineList().stream().map(e ->{
            return this.transfromColine(e,sqlResult.getScriptId(),sqlResult.getJobId(),sqlResult.getOutputTables().iterator().next());
        }).filter(Objects::nonNull).collect(Collectors.toList());

    }

    /**
     * 从ColLine转换成MetaColLineageDO
     * @param colLine
     * @return
     */
    private MetaColLineageDO transfromColine(ColLine colLine,Long scriptId,Long jobId,String outputTable){

        if(StringUtils.isEmpty(colLine.getToTable()) || StringUtils.isEmpty(colLine.getToNameParse())){
            //输出表 为空  应该是一个错误的解析
            logger.error("[{}]  error statement",colLine);
            return null;
        }

        String tableName = colLine.getToTable();

        if("TOK_TMP_FILE".equals(tableName) || StringUtils.isEmpty(tableName)){
            //tableName为TOK_TMP_FILE则解析有问题，直接把输出表作为字段名的前缀
            tableName = outputTable;
        }
        String plainColName = colLine.getToNameParse();

        /**
         * 字段名
         */
        String colName = Joiner.on(".").join(tableName,plainColName);

        ResourceColumnBO resourceColumnBO  = cacheService.getByColumnName(colName);

        if(null == resourceColumnBO){
            logger.error("字段[{}]找不到相应的资源id",colName);
        }

        MetaColLineageDO metaColLineageDO = new MetaColLineageDO();
        metaColLineageDO.setColResourceId(resourceColumnBO.getId());


        Set<String> directColNames = Sets.newHashSet();
        if(!CollectionUtils.isEmpty(colLine.getFromNameSet())){

            Set<String> directSet = colLine.getFromNameSet();
            directColNames = Sets.newHashSet(StringUtils.split(directSet.iterator().next(),"&"));
        }

        //处理where表达式和join表达式
        List<String> whereExpr = Lists.newArrayList();
        List<String> joinExpr = Lists.newArrayList();


        colLine.getConditionSet().stream().forEach(e ->{
            if(StringUtils.isEmpty(e)){
                return;
            }
            String[] splits = StringUtils.split(e,":");

            if(splits.length == 1){
                //MAPJOIN形式 不处理
                return;
            }

            if(splits[0].trim().startsWith("WHERE")){
                whereExpr.add(e);
            }else if(splits[0].trim().contains("JOIN")){
                joinExpr.add(e);
            }
        });

        Set<String> whereExprColNames = Sets.newHashSet();
        whereExpr.stream().map(colExtractUtil::getColsFromExpr).filter(Objects::nonNull).forEach(whereExprColNames::addAll);

        Set<String> joinExprColNames = Sets.newHashSet();
        joinExpr.stream().map(colExtractUtil::getColsFromExpr).filter(Objects::nonNull).forEach(joinExprColNames::addAll);

        //直接转换字段
        List<Long> directColIds = directColNames.stream().map(cacheService::getByColumnName).filter(Objects::nonNull).map(ResourceColumnBO::getId).collect(Collectors.toList());

        List<Long> whereExprColIds = whereExprColNames.stream().map(cacheService::getByColumnName).filter(Objects::nonNull).map(ResourceColumnBO::getId).collect(Collectors.toList());

        List<Long> joinExprColIds =  joinExprColNames.stream().map(cacheService::getByColumnName).filter(Objects::nonNull).map(ResourceColumnBO::getId).collect(Collectors.toList());

        //直接上游字段 是从fromNameSet + join表达式字段 + where表达式字段
        Set<Long> directUpSourceFields = Sets.newHashSet();
        directColIds.addAll(directColIds);
        directColIds.addAll(whereExprColIds);
        directColIds.addAll(joinExprColIds);

        metaColLineageDO.setColDirectUpSourceFields(Joiner.on(",").join(directUpSourceFields));

        //join表达式 把所有的join表达式以__分割存储
        metaColLineageDO.setColJoinExpr(Joiner.on("__").join(joinExpr));
        //where表达式 把所有的where表达式以__分割存储
        metaColLineageDO.setColWhereExpr(Joiner.on("__").join(whereExpr));

        metaColLineageDO.setColWhereSourceFields(Joiner.on(",").join(whereExprColIds));
        metaColLineageDO.setColJoinSourceFields(Joiner.on(",").join(joinExprColIds));


        //字段转换条件
        metaColLineageDO.setColConvertExpr(colLine.getColCondition());

        LineageFeature feature = new LineageFeature.LineageFeatureBuilder().withJobId(jobId).withScheduHistoryId(scriptId).build();

        metaColLineageDO.setFeature(feature.toString());

        metaColLineageDO.setColTemp(isTemporyFields(colName) ? 1:0);

        return metaColLineageDO;
    }


    /**
     *
     * @param colName
     * @return
     */
    private boolean isTemporyFields(String colName){
        String tableName = getTableNameFromColName(colName);
        if(StringUtils.isEmpty(tableName)){
            return false;
        }

        return temporyTables.contains(tableName);

    }

    private String getTableNameFromColName(String colName){
        String[]splits = StringUtils.split(colName,"\\.");
        if(splits.length != 3){
            return org.apache.commons.lang3.StringUtils.EMPTY;
        }

        return Joiner.on(".").join(splits[0],splits[1]);
    }

}

    
