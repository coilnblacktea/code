package com.vdian.bigdata.meta.lineage.processcycle;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  10:04
 **/


public interface ProcessCycleListener {

    /**
     * 判断是否是自己需要处理的事件
     * @param event
     * @return
     */
    boolean canHandle(ProcessCycleEvent event);

    /**
     * 处理事件
     * @param event
     */
    void processCycleEvent(ProcessCycleEvent event);


    /**
     * 每个流程处理完成后，可以向后发送消息
     */
    void fireEvent();
}
