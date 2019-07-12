package com.vdian.bigdata.meta.lineage.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.vdian.bigdata.meta.bo.ResourceColumnBO;
import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.lineage.cache.LineageCacheService;
import com.vdian.bigdata.meta.lineage.enums.LineageColTempEnums;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-08  15:32
 **/

@Service
public class LineageSourceFieldsServiceImpl implements LineageSourceFieldsService {

    @Autowired
    private LineageCacheService lineageCacheService;

    @Autowired
    private MetaColLineageMapper metaColLineageMapper;


    @Override
    @Transactional(transactionManager = "metaTransactionManager")
    public void processSourceFields() {


        List<Long> colResourceIds = metaColLineageMapper.getAllColIds();

        if(CollectionUtils.isEmpty(colResourceIds)){
            return ;
        }

        List<MetaColLineageDO> toUpdate = colResourceIds.stream().map(this::processSourceFields).filter(Objects::nonNull).collect(Collectors.toList());

        int update = metaColLineageMapper.updateSourceFields(toUpdate);
        if(update != toUpdate.size()){
            throw new MetaException("溯源字段的上游字段信息出错，未完全更新，回滚!", ErrorCode.SYSTEM_ERROR);
        }


    }


    private MetaColLineageDO processSourceFields(Long colId){

        MetaColLineageDO colResource = lineageCacheService.getMetaColByColId(colId);
        if(colResource == null){
            return null;
        }

        //去除临时表字段
        Set<Long> topSoureFields = Sets.newHashSet();
        //不去除临时表字段
        Set<Long> routeSourceFields = Sets.newHashSet();

        recursiveUpStreamFields(colResource,topSoureFields,routeSourceFields);

        //递归的时候 第一次把字段本身id加进去了，所以这里需要特殊处理一下
        topSoureFields.remove(colId);
        routeSourceFields.remove(colId);
        MetaColLineageDO result = new MetaColLineageDO();
        result.setColResourceId(colId);
        result.setTopSourceFields(Joiner.on(",").join(topSoureFields));
        result.setRouteSourceFields(Joiner.on(",").join(routeSourceFields));

        return result;
    }

    /**
     * 递归计算 字段的溯源上游字段
     * @param metaColLineageDO
     * @param topSourceFields
     * @param routeSourceFields
     */
    private void recursiveUpStreamFields(MetaColLineageDO metaColLineageDO,Set<Long> topSourceFields,Set<Long> routeSourceFields){
        routeSourceFields.add(metaColLineageDO.getColResourceId());
        if(metaColLineageDO.getColTemp() == LineageColTempEnums.NOT_TEMP.getCode()){
            //不是临时表字段
            topSourceFields.add(metaColLineageDO.getColResourceId());
        }

        if(StringUtils.isEmpty(metaColLineageDO.getColDirectUpSourceFields())){
            return;
        }
        String[]splits = StringUtils.split(metaColLineageDO.getColDirectUpSourceFields(),",");

        Arrays.asList(splits).stream().forEach(e ->{
            MetaColLineageDO colResource = lineageCacheService.getMetaColByColId(Long.valueOf(e));
            if(colResource == null){
                return ;
            }
            recursiveUpStreamFields(colResource,topSourceFields,routeSourceFields);
        });

    }


    @Override
    public MetaColLineageDO getByColName(String colName){
        ResourceColumnBO resourceColumnBO = lineageCacheService.getByColumnName(colName);
        if(null == resourceColumnBO){
            throw new MetaException(String.format("can't find col by name %s",colName),ErrorCode.PARAM_ERROR);
        }
        return processSourceFields(resourceColumnBO.getId());
    }
}

    
