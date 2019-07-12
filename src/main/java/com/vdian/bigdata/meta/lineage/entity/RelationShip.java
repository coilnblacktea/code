package com.vdian.bigdata.meta.lineage.entity;

import lombok.ToString;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;
import java.util.Map;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-27  16:37
 **/


public class RelationShip {

    private Long node1Id;

    private Long node2Id;


    private String table;

    private Map<String, List<String>> propertyMap;


    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }


    public Long getNode1Id() {
        return node1Id;
    }

    public void setNode1Id(Long node1Id) {
        this.node1Id = node1Id;
    }

    public Long getNode2Id() {
        return node2Id;
    }

    public void setNode2Id(Long node2Id) {
        this.node2Id = node2Id;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, List<String>> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, List<String>> propertyMap) {
        this.propertyMap = propertyMap;
    }
}

    
