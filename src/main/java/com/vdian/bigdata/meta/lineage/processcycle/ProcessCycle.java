package com.vdian.bigdata.meta.lineage.processcycle;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  09:45
 **/


public interface ProcessCycle {

    /**
     *
     * @param listener
     */
    void addProcessCycleListener(ProcessCycleListener listener);

    /**
     *
     * @param listener
     */
    void removeProcessCycleListener(ProcessCycleListener listener);


    /**
     *
     * @param processCycleEvent
     */
    void fireProcessEvent(ProcessCycleEvent processCycleEvent);

}
