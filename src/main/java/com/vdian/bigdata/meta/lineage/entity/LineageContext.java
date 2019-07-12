package com.vdian.bigdata.meta.lineage.entity;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;
import java.util.function.Supplier;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  17:28
 **/


public class LineageContext {


    private static ThreadLocal<LineageContext> local = ThreadLocal.withInitial(() -> new LineageContext());
    /**
     * 解析出错的任务的调度id
     */
    private List<Long> parseErrorScriptId = Lists.newArrayList();


    private List<Long> parseEmtpyScriptId = Lists.newArrayList();

    /**
     * 当次解析的所有任务
     */
    private Long totalParsedScripts = 0L;

    /**
     * 一次解析花费的时长
     */
    private Long allParseTime = 0L;

    public List<Long> getParseErrorScriptId() {
        return parseErrorScriptId;
    }

    public void setParseErrorScriptId(List<Long> parseErrorScriptId) {
        this.parseErrorScriptId = parseErrorScriptId;
    }

    public Long getTotalParsedScripts() {
        return totalParsedScripts;
    }

    public void setTotalParsedScripts(Long totalParsedScripts) {
        this.totalParsedScripts = totalParsedScripts;
    }

    public Long getAllParseTime() {
        return allParseTime;
    }

    public void setAllParseTime(Long allParseTime) {
        this.allParseTime = allParseTime;
    }

    public List<Long> getParseEmtpyScriptId() {
        return parseEmtpyScriptId;
    }

    public void setParseEmtpyScriptId(List<Long> parseEmtpyScriptId) {
        this.parseEmtpyScriptId = parseEmtpyScriptId;
    }

    /**
     * @return
     */
    public static LineageContext getInstance() {
        return local.get();
    }

    /**
     *
     */
    public static void clear() {
        local.remove();
    }


    public void increParseScript(){
        this.totalParsedScripts++;
    }

    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

}


    
