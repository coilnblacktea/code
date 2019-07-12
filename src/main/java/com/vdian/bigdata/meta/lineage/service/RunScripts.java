package com.vdian.bigdata.meta.lineage.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import com.vdian.bigdata.meta.lineage.enums.SqlSourceEnums;
import com.vdian.bigdata.meta.lineage.util.DateUtil;
import com.vdian.bigdata.meta.mars.domain.ScheduleDO;
import com.vdian.bigdata.meta.mars.mapper.ScheduleMapper;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-02  11:38
 **/

@Service
public class RunScripts {


    @Autowired
    private ScheduleMapper scheduleMapper;

    private static final Logger logger = LoggerFactory.getLogger(RunScripts.class);


    @Autowired
    private LineageSqlParser lineageSqlParser;

    /**
     *
     * @param jobId
     */
    public List<SQLResult> runScriptByJobId(Long jobId){

        ScheduleDO scheduleDO = scheduleMapper.getLastScheSusscessJobsByJobId(jobId);

        List<SQLResult> result = lineageSqlParser.parseSql(scheduleDO.getScript(),SqlSourceEnums.HIVE);
        logger.error("result is [{}]",new Gson().toJson(result));

        return result;
    }
    public void runScript(Integer interval){

        Date startDate = DateUtil.addDays(new Date(),-interval);

        Date endDate = DateUtil.addDays(new Date(),-1);

        List<ScheduleDO> scheduleDOList = scheduleMapper.listScheSuccsJobs(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(startDate),new SimpleDateFormat("yyyy-MM-dd").format(endDate)+" 59:59:59");


        List<Long> errorIds = Lists.newArrayList();
        logger.error("[parseSqlTotal] 符合条件的sql一共有[{}]个",scheduleDOList.size());
        long start = System.currentTimeMillis();
        for(ScheduleDO scheduleDO: scheduleDOList){
            try{
                if(StringUtils.isEmpty(scheduleDO.getScript())){
                    logger.error("[scheduScriptEmpty] scheduleId is[{}]",scheduleDO.getId());
                    continue;
                }
                List<SQLResult> results = lineageSqlParser.parseSql(scheduleDO.getScript(), SqlSourceEnums.HIVE);

                if(CollectionUtils.isEmpty(results)){
                    logger.error("[parseSqlEmpty] script id is [{}]",scheduleDO.getId());
                }


            }catch(Exception e){
                logger.error("error script is [{}]",scheduleDO.getScript());
                errorIds.add(scheduleDO.getId());
            }
        }

        logger.error("[parseSqlError] all [{}]  errors [{}], id列表[{}]",scheduleDOList.size(),errorIds.size(), Joiner.on(",").join(errorIds));
        logger.error("[parseSqlTook]  一共解析了[{}]秒",(System.currentTimeMillis() - start)/1000);
    }
}

    
