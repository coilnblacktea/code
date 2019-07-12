package com.vdian.bigdata.meta.lineage.service;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.lineage.processcycle.LineageProceeCycle;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleEvent;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleListener;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleState;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-08  11:52
 **/

@Service
public class LineageDownStreamRecursiveServiceImpl implements LineageDownstreamRecursiveService, ProcessCycleListener {


    @Autowired
    private MetaColLineageMapper metaColLineageMapper;

    @Autowired
    private LineageProceeCycle lineageProceeCycle;



    private Integer message;

    @PostConstruct
    public void init(){
        lineageProceeCycle.addProcessCycleListener(this);
    }

    @Override
    public void downStreamRecursive() {

        List<Long> allResourceIds = metaColLineageMapper.getAllColIds();

        if(CollectionUtils.isEmpty(allResourceIds)){
            return ;
        }

        List<MetaColLineageDO> toUpdate = allResourceIds.stream().map(this::processDownStreamInfo).filter(Objects::nonNull).collect(Collectors.toList());


        if(CollectionUtils.isEmpty(toUpdate)){
            return;
        }

        Integer update = metaColLineageMapper.updateDownStreamInfos(toUpdate);
        if(update != toUpdate.size()){
            throw new MetaException("处理字段的下游信息出错，未完全更新，回滚!", ErrorCode.SYSTEM_ERROR);
        }

        //溯源完成后 需要计算字段的pv
        fireEvent();

    }


    private MetaColLineageDO processDownStreamInfo(Long colId){
        List<MetaColLineageDO> downStreamInfos = metaColLineageMapper.getDirectUpContainsColIdInfo(colId);
        if(CollectionUtils.isEmpty(downStreamInfos)){
            return null;
        }

        MetaColLineageDO metaColLineageDO = new MetaColLineageDO();
        metaColLineageDO.setColResourceId(colId);

        List<Long> downStreamIds = Lists.newArrayList();

        downStreamInfos.stream().forEach(e ->{

            String[] upStreamIds = StringUtils.split(e.getColDirectUpSourceFields(),",");

            for(String id: upStreamIds){
                if(Long.valueOf(id).equals(colId)){
                    downStreamIds.add(e.getColResourceId());
                    return;
                }
            }
        });


        metaColLineageDO.setColDirectDownSourceFields(Joiner.on(",").join(downStreamIds));

        return metaColLineageDO;
    }

    @Override
    public boolean canHandle(ProcessCycleEvent event) {
        if(event.getState().equals(ProcessCycleState.SCRIPT_ANALYZED)){
            this.message = (Integer) event.getMessage();
            lineageProceeCycle.setState(ProcessCycleState.LINEAGE_ANALYZING);
            return true;
        }

        return false;
    }

    @Override
    @Transactional(transactionManager = "metaTransactionManager")
    public void processCycleEvent(ProcessCycleEvent event) {
        if(canHandle(event)){
            downStreamRecursive();
        }
    }

    @Override
    public void fireEvent() {
        lineageProceeCycle.fireProcessEvent(new ProcessCycleEvent(ProcessCycleState.LINEAGE_ANALYZED,message));

    }
}

    
