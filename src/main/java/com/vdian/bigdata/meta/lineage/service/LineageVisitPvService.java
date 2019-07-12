package com.vdian.bigdata.meta.lineage.service;

import com.vdian.bigdata.meta.meta.domain.lineage.MetaColAccessDO;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-09  10:47
 **/


public interface LineageVisitPvService {


    /**
     *
     * @param interval
     */
    void countVisitPv(Integer interval);

    /**
     *
     * @param colName
     * @return
     */
    MetaColAccessDO countVisitPv(String colName);
}
