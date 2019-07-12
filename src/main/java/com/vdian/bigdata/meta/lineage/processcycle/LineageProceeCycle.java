package com.vdian.bigdata.meta.lineage.processcycle;

import org.springframework.stereotype.Service;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  10:15
 **/


@Service
public class LineageProceeCycle extends AbstractProcessCycle {

    private ProcessCycleState state;


    public ProcessCycleState getState() {
        return state;
    }

    public void setState(ProcessCycleState state) {
        this.state = state;
    }
}

    
