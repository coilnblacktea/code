package com.vdian.bigdata.meta.lineage.processcycle;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  10:05
 **/


public abstract  class AbstractProcessCycle implements  ProcessCycle {

    private List<ProcessCycleListener> listeners = new CopyOnWriteArrayList<>();


    @Override
    public void addProcessCycleListener(ProcessCycleListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeProcessCycleListener(ProcessCycleListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void fireProcessEvent(ProcessCycleEvent processCycleEvent) {

        listeners.stream().forEach(e ->{
            e.processCycleEvent(processCycleEvent);
        });
    }


}

    
