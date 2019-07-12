package com.vdian.bigdata.meta.lineage.service;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.adhoc.mapper.AdhocMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-09  10:13
 **/

@Service
public class AdhocScriptFetcher {



    @Autowired
    private AdhocMapper adhocMapper;

    private Integer counter = 1;


    private Long total = Long.MAX_VALUE;

    private static final Integer PAGE_SIZE = 300;

    public List<String> fetch(String startTime, String endTime){


        if(judgeFinish()){
            reset();
            return Lists.newArrayList();
        }

        PageHelper.startPage(counter++,PAGE_SIZE);
        List<String> doResult = adhocMapper.getAdhocSuccSqls(startTime,endTime);

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

    
