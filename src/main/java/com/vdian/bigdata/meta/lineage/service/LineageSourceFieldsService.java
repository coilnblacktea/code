package com.vdian.bigdata.meta.lineage.service;

import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-08  15:31
 **/


public interface LineageSourceFieldsService {


    /**
     * 处理TopSourceFields和RouteSourceFields
     */
    void processSourceFields();


    /**
     * 排查问题需要的接口
     * @param colName
     * @return
     */
    MetaColLineageDO getByColName(String colName);

}

    
