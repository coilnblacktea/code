package com.vdian.bigdata.meta.lineage.service;

import com.vdian.bigdata.meta.lineage.entity.LineageContext;
import com.vdian.bigdata.meta.lineage.processcycle.LineageProceeCycle;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleEvent;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleListener;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  11:46
 **/

@Service
public class LineageProcessEnd implements ProcessCycleListener {

    @Autowired
    private LineageProceeCycle lineageProceeCycle;


    @PostConstruct
    public void init(){
        lineageProceeCycle.addProcessCycleListener(this);
    }



    @Override
    public boolean canHandle(ProcessCycleEvent event) {
        if(event.getState().equals(ProcessCycleState.LINEAGE_SIMILARITY_PROCESSED)){
            lineageProceeCycle.setState(ProcessCycleState.LINEAGE_PROCESS_END);

            return true;
        }

        return false;
    }

    @Override
    public void processCycleEvent(ProcessCycleEvent event) {
        canHandle(event);

        //一次跑完以后，把context清空
        LineageContext.clear();
    }

    @Override
    public void fireEvent() {

    }
}

    
