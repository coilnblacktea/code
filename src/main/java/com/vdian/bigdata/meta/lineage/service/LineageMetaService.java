package com.vdian.bigdata.meta.lineage.service;

import com.vdian.bigdata.meta.bo.ResourceColumnBO;

import java.util.List;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-28  14:38
 **/


public interface LineageMetaService {


    /**
     *
     * @param tableName
     * @return
     */
    List<ResourceColumnBO> getColumnDetailsByTable(String tableName);
    /**
     * 根据表名拿表的字段
     * @param tableName
     * @return
     */
    List<String> getByTable(String tableName);


    /**
     * 表是否存在
     * @param tableName
     * @return
     */
    boolean isTableExsits(String tableName);


    /**
     *
     * @param columnId
     * @return
     */
    ResourceColumnBO getByColumnId(Long columnId);

}
