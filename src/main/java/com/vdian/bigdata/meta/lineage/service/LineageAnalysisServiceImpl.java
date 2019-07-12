package com.vdian.bigdata.meta.lineage.service;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.vdian.bigdata.meta.lineage.entity.LineageContext;
import com.vdian.bigdata.meta.lineage.enums.SqlSourceEnums;
import com.vdian.bigdata.meta.lineage.processcycle.*;
import com.vdian.bigdata.meta.lineage.util.DateUtil;
import com.vdian.bigdata.meta.mars.domain.ScheduleDO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  17:47
 **/

@Service
public class LineageAnalysisServiceImpl implements LineageAnalysisService, ProcessCycleListener {


    private static final Logger logger = LoggerFactory.getLogger(LineageAnalysisServiceImpl.class);

    @Autowired
    private LineageSqlParser lineageSqlParser;

    @Autowired
    private MarsScheduleScriptFetcher scriptFetcher;

    @Autowired
    private LineageProceeCycle lineageProceeCycle;


    private Integer interval;


    @PostConstruct
    public void init(){
        lineageProceeCycle.addProcessCycleListener(this);
    }


    @Override
    public void buildAll(Integer interval) {

        Stopwatch stopwatch = new Stopwatch();
        try {
            stopwatch.start();
            process(interval, 1);
        }finally {
            stopwatch.stop();
            LineageContext.getInstance().setAllParseTime(stopwatch.elapsed(TimeUnit.MINUTES));
            if(logger.isInfoEnabled()){
                logger.info("buildAll interval is [{}] context is [{}]",interval,LineageContext.getInstance());
            }
            LineageContext.clear();
        }
        /**
         * 血缘关系解析完后，发送消息进行回溯
         */
        fireEvent();

    }

    /**
     * 处理完成后 发送消息
     */
    @Override
    public void fireEvent(){
        ProcessCycleEvent event = new ProcessCycleEvent(ProcessCycleState.SCRIPT_ANALYZED,interval);
        lineageProceeCycle.fireProcessEvent(event);
    }


    /**
     * @param start
     * @param end
     */
    private void process(Integer start, Integer end) {
        String startTime = Joiner.on(" ").join(getProcessTime(start), "00:00:00");

        String endTime = Joiner.on(" ").join(getProcessTime(end), "59:59:59");

        List<ScheduleDO> toProcessList = scriptFetcher.fetch(startTime, endTime);

        while (!CollectionUtils.isEmpty(toProcessList)) {
            lineageSqlParser.batchParseSql(toProcessList, SqlSourceEnums.HIVE);
            toProcessList = scriptFetcher.fetch(startTime,endTime);
        }

    }

    /**
     * 处理时间格式化
     * @param interval
     * @return
     */
    private String getProcessTime(Integer interval) {
        Date date = DateUtil.addDays(new Date(), -interval);

        return new SimpleDateFormat("yyyy-MM-dd").format(date);

    }

    @Override
    public void autoParse() {
        Stopwatch stopwatch = new Stopwatch();
        try {
            stopwatch.start();
            process(1, 1);
        }finally {
            stopwatch.stop();
            LineageContext.getInstance().setAllParseTime(stopwatch.elapsed(TimeUnit.MINUTES));
            if(logger.isInfoEnabled()){
                logger.info("autoParse time is [{}] context is [{}]",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),LineageContext.getInstance());
            }
            LineageContext.clear();
        }
    }

    @Override
    public boolean canHandle(ProcessCycleEvent event) {

        if(event.getState().equals(ProcessCycleState.NEW)){
            lineageProceeCycle.setState(ProcessCycleState.SCRIPT_ANALYZING);
            return true;
        }

        return false;
    }

    @Override
    public void processCycleEvent(ProcessCycleEvent event) {
        if(canHandle(event)){
            interval = (Integer) event.getMessage();
            buildAll(interval);
        }
    }
}

    
