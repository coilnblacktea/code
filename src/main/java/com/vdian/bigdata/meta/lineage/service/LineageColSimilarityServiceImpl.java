package com.vdian.bigdata.meta.lineage.service;

import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.lineage.processcycle.LineageProceeCycle;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleEvent;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleListener;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleState;
import com.vdian.bigdata.meta.lineage.util.MathUtils;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColSimilarityDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColSimilarityMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  11:42
 **/

@Service
public class LineageColSimilarityServiceImpl implements LineageColSimilarityService, ProcessCycleListener {



    private static final Logger  logger = LoggerFactory.getLogger(LineageColSimilarityService.class);


    @Autowired
    private LineageProceeCycle lineageProceeCycle;
    private Integer message;


    @Autowired
    private MetaColSimilarityMapper metaColSimilarityMapper;


    @Autowired
    private MetaColLineageMapper metaColLineageMapper;


    @PostConstruct
    public void init(){
        lineageProceeCycle.addProcessCycleListener(this);
    }

    @Override
    public void calculateSimilarity() {

        /**
         * 拿到所有的lineage信息，后面计算
         */
        List<MetaColLineageDO> toCacculates = metaColLineageMapper.selectForSimilarityCaculate();

        List<MetaColSimilarityDO> similarityDOS = Lists.newArrayList();

        for(int i = 0; i < toCacculates.size(); i++){
            for(int j = i+1; j < toCacculates.size(); j++){
                MetaColLineageDO source = toCacculates.get(i);
                MetaColLineageDO target = toCacculates.get(j);
                similarityDOS.add(cacculateSimilarity(source,target));
            }
        }

        metaColSimilarityMapper.deleteAll();

       int inserts =  metaColSimilarityMapper.batchInsert(similarityDOS);

       if(inserts != similarityDOS.size()){
           logger.error("insert similarity error, all [{}] ,insert [{}}, rollback ",similarityDOS.size(),inserts);
           throw new MetaException("插入相似度出错，回滚", ErrorCode.SYSTEM_ERROR);
       }

       lineageProceeCycle.setState(ProcessCycleState.LINEAGE_SIMILARITY_PROCESSED);
    }

    /**
     * 计算两个字段的相似度
     * @param source
     * @param target
     * @return
     */
    private MetaColSimilarityDO cacculateSimilarity(MetaColLineageDO source, MetaColLineageDO target) {

        MetaColSimilarityDO result = new MetaColSimilarityDO();
        result.setColResourceIdSource(source.getColResourceId());
        result.setColResourceIdTarget(target.getColResourceId());


        String sourceTop = source.getTopSourceFields();
        String targetTop = target.getTopSourceFields();

        result.setColTopSimilarity(cauculate(sourceTop,targetTop));

        String sourceRoute = source.getRouteSourceFields();
        String targetRoute = target.getRouteSourceFields();

        result.setColRouteSimilarity(cauculate(sourceRoute,targetRoute));
        return result;
    }

    /**
     * 计算两个字段的相似度
     * @param source
     * @param target
     * @return
     */
    private double cauculate(String source,String target){

        if(StringUtils.isEmpty(source) || StringUtils.isEmpty(target)){
            return 0;
        }

        Set<Long> sourceIds = Arrays.asList(StringUtils.split(source,",")).stream().map(Long::valueOf).collect(Collectors.toSet());

        Set<Long> targetIds = Arrays.asList(StringUtils.split(target,",")).stream().map(Long::valueOf).collect(Collectors.toSet());

        Collection<Long> commonIds = CollectionUtils.intersection(sourceIds,targetIds);

        if(CollectionUtils.isEmpty(commonIds)){
            return 0;
        }

        return MathUtils.divide(commonIds.size()*2,sourceIds.size() + targetIds.size());

    }
    @Override
    public boolean canHandle(ProcessCycleEvent event) {
        if(event.getState().equals(ProcessCycleState.LINEAGE_PV_COUNTED)){
            lineageProceeCycle.setState(ProcessCycleState.LINEAGE_SIMILARITY_PROCESSING);
            this.message = (Integer) event.getMessage();
            return true;
        }

        return false;
    }

    @Override
    @Transactional(transactionManager = "metaTransactionManager")
    public void processCycleEvent(ProcessCycleEvent event) {
        if(canHandle(event)){
            calculateSimilarity();
        }

    }

    @Override
    public void fireEvent() {
        lineageProceeCycle.fireProcessEvent(new ProcessCycleEvent(ProcessCycleState.LINEAGE_SIMILARITY_PROCESSED));
    }
}

    
