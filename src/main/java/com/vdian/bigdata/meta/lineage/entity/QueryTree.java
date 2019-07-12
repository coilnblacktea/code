package com.vdian.bigdata.meta.lineage.entity;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;
import java.util.Set;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-27  16:34
 **/


public class QueryTree {

    /**
     * 当前子查询节点id
     */
    private Integer id;

    /**
     * 父节点子查询树id
     */
    private Integer pId;

    private String current;

    /**
     * 父节点的名字
     */
    private String parent;

    private Set<String> tableSet = Sets.newHashSet();


    private List<QueryTree> childList = Lists.newArrayList();

    private List<ColLine> colLineList = Lists.newArrayList();


    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getpId() {
        return pId;
    }

    public void setpId(Integer pId) {
        this.pId = pId;
    }

    public String getCurrent() {
        return current;
    }

    public void setCurrent(String current) {
        this.current = current;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public Set<String> getTableSet() {
        return tableSet;
    }

    public void setTableSet(Set<String> tableSet) {
        this.tableSet = tableSet;
    }

    public List<QueryTree> getChildList() {
        return childList;
    }

    public void setChildList(List<QueryTree> childList) {
        this.childList = childList;
    }

    public List<ColLine> getColLineList() {
        return colLineList;
    }

    public void setColLineList(List<ColLine> colLineList) {
        this.colLineList = colLineList;
    }
}

    
