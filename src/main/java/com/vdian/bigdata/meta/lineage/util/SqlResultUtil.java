package com.vdian.bigdata.meta.lineage.util;

import com.google.common.collect.Sets;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * @author: lhy
 * @description 处理SQl解析结果的工具类
 * @created: 2019-07-01  17:20
 **/

@Service
public class SqlResultUtil {


    /**
     * 从sql解析结果中识别出临时表
     * @param sqlResults
     * @return
     */
    public Set<String>  identifyTemporyTables(List<SQLResult> sqlResults){

        if(CollectionUtils.isEmpty(sqlResults)){
            return Sets.newHashSet();
        }

        Set<String> inputTables = Sets.newHashSet();
        Set<String> outputTables = Sets.newHashSet();
        sqlResults.stream().forEach(e ->{
            inputTables.addAll(e.getInputTables());
            outputTables.addAll(e.getOutputTables());
        });

        return Sets.intersection(inputTables,outputTables);

    }

    /**
     * 是否只是查询语句  （所有sql查询的outputTables均为空）
     * @param sqlResults
     * @return
     */
    public boolean isPureSelect(List<SQLResult> sqlResults){
        if(CollectionUtils.isEmpty(sqlResults)){
            return false;
        }

        return sqlResults.stream().allMatch(e ->{
            return CollectionUtils.isEmpty(e.getOutputTables());
        });

    }



}

    
