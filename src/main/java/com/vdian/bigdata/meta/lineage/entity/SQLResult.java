package com.vdian.bigdata.meta.lineage.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;
import java.util.Set;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-27  16:40
 **/


public class SQLResult {

    private Set<String> outputTables;

    private Set<String> inputTables;

    private List<ColLine> colLineList;

    /**
     * 调度id
     */
    private Long scriptId;

    /**
     * 任务id
     */
    private Long jobId;




    public Long getScriptId() {
        return scriptId;
    }

    public void setScriptId(Long scriptId) {
        this.scriptId = scriptId;
    }


    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    public Set<String> getOutputTables() {
        return outputTables;
    }

    public void setOutputTables(Set<String> outputTables) {
        this.outputTables = outputTables;
    }

    public Set<String> getInputTables() {
        return inputTables;
    }

    public void setInputTables(Set<String> inputTables) {
        this.inputTables = inputTables;
    }

    public List<ColLine> getColLineList() {
        return colLineList;
    }

    public void setColLineList(List<ColLine> colLineList) {
        this.colLineList = colLineList;
    }
}

    
