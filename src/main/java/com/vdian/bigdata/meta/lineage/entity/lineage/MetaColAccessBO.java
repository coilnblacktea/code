package com.vdian.bigdata.meta.lineage.entity.lineage;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  10:13
 **/
@ApiModel("字段访问次数表")
public class MetaColAccessBO extends  MetaLineageBaseBO {

    @ApiModelProperty("字段访问热度")
    private Long colVisitPv;

    @ApiModelProperty("字段访问价值度")
    private Long colSumVisitPv;

    public Long getColVisitPv() {
        return colVisitPv;
    }

    public void setColVisitPv(Long colVisitPv) {
        this.colVisitPv = colVisitPv;
    }

    public Long getColSumVisitPv() {
        return colSumVisitPv;
    }

    public void setColSumVisitPv(Long colSumVisitPv) {
        this.colSumVisitPv = colSumVisitPv;
    }
}

    
