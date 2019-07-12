package com.vdian.bigdata.meta.lineage.service;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-09  17:22
 **/

@Service
public class MetaLineageDownStreamFetcher {

    @Autowired
    private MetaColLineageMapper metaColLineageMapper;

    private Integer counter = 1;

    private Long total = Long.MAX_VALUE;

    private static final Integer PAGE_SIZE = 300;

    public List<MetaColLineageDO> fetch(){
        if(judgeFinish()){
            reset();
            return Lists.newArrayList();
        }

        PageHelper.startPage(counter++,PAGE_SIZE);
        List<MetaColLineageDO> doResult = metaColLineageMapper.getDirectDownInfos();
        if(CollectionUtils.isEmpty(doResult)){
            reset();
            return Lists.newArrayList();
        }

        if(doResult instanceof  Page){
            Page page = (Page) doResult;
            this.total = page.getTotal();
        }


        return doResult;

    }


    private void reset(){
        total = Long.MAX_VALUE;
        counter = 1;
    }

    private boolean judgeFinish(){
        return counter > total;
    }


}

    
