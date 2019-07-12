package com.vdian.bigdata.meta.lineage.entity;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Set;

/**
 * @author: lhy
 * @description 解析的sql块
 * @created: 2019-06-27  15:53
 **/


public class Block {


    private String condition;

    private Set<String> colSet = Sets.newLinkedHashSet();



    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }


    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public Set<String> getColSet() {
        return colSet;
    }

    public void setColSet(Set<String> colSet) {
        this.colSet = colSet;
    }
}

    
