package com.vdian.bigdata.meta.lineage.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-27  16:14
 **/


public class ColumnNode {

    private Long id;

    private String column;

    private Long tableId;

    private String table;

    private String db;


    @Override
    public String toString(){

        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}

    
