package com.vdian.bigdata.meta.lineage.service;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-08  11:50
 **/

public interface LineageDownstreamRecursiveService {


    /**
     * 血缘分析完成后 更新字段的下游信息
     */
    public void downStreamRecursive();

}

    
