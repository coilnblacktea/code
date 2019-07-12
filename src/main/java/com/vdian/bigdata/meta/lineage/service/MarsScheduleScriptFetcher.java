package com.vdian.bigdata.meta.lineage.service;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.mars.domain.ScheduleDO;
import com.vdian.bigdata.meta.mars.mapper.ScheduleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  18:37
 **/


@Service
public class MarsScheduleScriptFetcher {


    private static final Logger logger = LoggerFactory.getLogger(MarsScheduleScriptFetcher.class);

    @Autowired
    private ScheduleMapper scheduleMapper;

    /**
     * 当前取到第几页
     */
    private Integer counter = 1;

    /**
     * 一共有多少页
     */
    private Long total = Long.MAX_VALUE;

    private static final Integer PAGE_SIZE = 300;


    public List<ScheduleDO> fetch(String startTime,String endTime){

       if(logger.isInfoEnabled()) {
           logger.info("startTime is [{}], endTime is [{}] ,current page is [{}], total is [{}]", startTime, endTime, counter, total);
       }

       if(judgeFinish()){
           if(logger.isInfoEnabled()) {
               logger.info("[{}] fetech all, now reset", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
           }
           reset();
           return Lists.newArrayList();
       }

       PageHelper.startPage(counter++,PAGE_SIZE);
       List<ScheduleDO> doResult = scheduleMapper.listScheSuccsJobs(startTime,endTime);

       if(CollectionUtils.isEmpty(doResult)){
           reset();
           return Lists.newArrayList();
       }

       //每次取出后更新total
       if(doResult instanceof Page){
           Page page = (Page)doResult;
           this.total = page.getTotal();
       }

       return doResult;
    }

    /**
     *重置变量
     */
    private void reset(){
        //如果取完了，则counter归1，total重新设置
        total = Long.MAX_VALUE;
        counter = 1;
    }

    /**
     * 判断是否取完了
     * @return
     */
    private boolean judgeFinish(){
        return counter > total;
    }




}

    
