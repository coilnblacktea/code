package com.vdian.bigdata.meta.lineage.entity;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Arrays;
import java.util.Set;

/**
 * @author: lhy
 * @description 生成列的血缘关系
 * @created: 2019-06-27  15:55
 **/
public class ColLine {

    /**
     * 解析sql出来的列名称
     */
    private String toNameParse;

    /**
     * 待条件的源字段
     */
    private String colCondition;

    /**
     * 源字段
     */
    private Set<String> fromNameSet = Sets.newLinkedHashSet();

    /**
     * 计算条件
     */
    private Set<String> conditionSet = Sets.newLinkedHashSet();

    private Set<String> allConditionSet = Sets.newHashSet();

    /**
     * 解析出来输出表
     */
    private String toTable;

    /**
     * 查询元数据出来的名称
     */
    private String toName;


    private static final String COL_COLFUN = "COLFUN:";

    public ColLine(){

    }

    public ColLine(String toNameParse,String colCondition,Set<String>fromNameSet,Set<String>conditionSet,String toTable,String toName){
        this.toNameParse = toNameParse;
        this.colCondition = colCondition;
        this.fromNameSet = fromNameSet;
        this.conditionSet = conditionSet;
        this.toTable = toTable;
        this.toName = toName;
    }


    /**
     *
     * @return
     */
    public Set<String> getAllConditionSet(){
        allConditionSet.clear();

        if(needAdd()){
            allConditionSet.add(COL_COLFUN + colCondition);
        }

        allConditionSet.addAll(conditionSet);

        return allConditionSet;

    }

    private boolean needAdd(){
        if(StringUtils.isNotEmpty(colCondition)){
            if(CollectionUtils.isEmpty(fromNameSet)){
                return true;
            }
        }

        String[] splits = StringUtils.split(colCondition,"&");

        if(null != splits && splits.length > 0){
            return Arrays.asList(splits).stream().anyMatch(e ->{
                return !fromNameSet.contains(e);
            });
        }

        return false;
    }



    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    public String getToNameParse() {
        return toNameParse;
    }

    public void setToNameParse(String toNameParse) {
        this.toNameParse = toNameParse;
    }

    public String getColCondition() {
        return colCondition;
    }

    public void setColCondition(String colCondition) {
        this.colCondition = colCondition;
    }

    public Set<String> getFromNameSet() {
        return fromNameSet;
    }

    public void setFromNameSet(Set<String> fromNameSet) {
        this.fromNameSet = fromNameSet;
    }

    public Set<String> getConditionSet() {
        return conditionSet;
    }

    public void setConditionSet(Set<String> conditionSet) {
        this.conditionSet = conditionSet;
    }



    public void setAllConditionSet(Set<String> allConditionSet) {
        this.allConditionSet = allConditionSet;
    }

    public String getToTable() {
        return toTable;
    }

    public void setToTable(String toTable) {
        this.toTable = toTable;
    }

    public String getToName() {
        return toName;
    }

    public void setToName(String toName) {
        this.toName = toName;
    }


}

    
