package com.vdian.bigdata.meta.lineage.processcycle;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  09:46
 **/
public final class ProcessCycleEvent<T> {


    private ProcessCycleState state;

    /**
     * 事件中可能需要带一些参数
     */
    private T message;


    /**
     * 有些事件没有参数
     * @param state
     */
    public ProcessCycleEvent(ProcessCycleState state){
        this.state = state;
    }

    /**
     * 带参数的事件消息
     * @param state
     * @param message
     */
    public ProcessCycleEvent(ProcessCycleState state,T message){
        this.state = state;
        this.message = message;
    }


    public T getMessage() {
        return message;
    }

    public ProcessCycleState getState() {
        return state;
    }
}

    
