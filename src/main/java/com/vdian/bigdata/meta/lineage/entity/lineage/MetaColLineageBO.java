package com.vdian.bigdata.meta.lineage.entity.lineage;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  10:14
 **/

@ApiModel("血缘关系实体")
public class MetaColLineageBO extends  MetaLineageBaseBO{


    @ApiModelProperty("字段的直接上游字段")
    private String colDirectUpSourceFields;

    @ApiModelProperty("字段的直接下游字段")
    private String colDirectDownSourceFields;

    @ApiModelProperty("join表达式中的字段")
    private String colJoinSourceFields;

    @ApiModelProperty("where表达式中的字段")
    private String colWhereSourceFields;

    @ApiModelProperty("join表达式")
    private String colJoinExpr;

    @ApiModelProperty("where表达式")
    private String colWhereExpr;

    @ApiModelProperty("字段转换条件")
    private String colConvertExpr;

    @ApiModelProperty("是否是临时表字段")
    private Integer colTemp;

    @ApiModelProperty("字段的剔除临时表字段的所有上游字段")
    private String topSourceFields;

    @ApiModelProperty("字段的所有上游字段（不剔除临时表字段）")
    private String routeSourceFields;

    @ApiModelProperty("字段的热度")
    private Long colVisitPv;

    @ApiModelProperty("字段价值度")
    private Long colSumVisitPv;

    @ApiModelProperty("扩展字段")
    private String feature;

    public String getColDirectUpSourceFields() {
        return colDirectUpSourceFields;
    }

    public void setColDirectUpSourceFields(String colDirectUpSourceFields) {
        this.colDirectUpSourceFields = colDirectUpSourceFields;
    }

    public String getColDirectDownSourceFields() {
        return colDirectDownSourceFields;
    }

    public void setColDirectDownSourceFields(String colDirectDownSourceFields) {
        this.colDirectDownSourceFields = colDirectDownSourceFields;
    }

    public String getColJoinSourceFields() {
        return colJoinSourceFields;
    }

    public void setColJoinSourceFields(String colJoinSourceFields) {
        this.colJoinSourceFields = colJoinSourceFields;
    }

    public String getColWhereSourceFields() {
        return colWhereSourceFields;
    }

    public void setColWhereSourceFields(String colWhereSourceFields) {
        this.colWhereSourceFields = colWhereSourceFields;
    }

    public String getColJoinExpr() {
        return colJoinExpr;
    }

    public void setColJoinExpr(String colJoinExpr) {
        this.colJoinExpr = colJoinExpr;
    }

    public String getColWhereExpr() {
        return colWhereExpr;
    }

    public void setColWhereExpr(String colWhereExpr) {
        this.colWhereExpr = colWhereExpr;
    }

    public String getColConvertExpr() {
        return colConvertExpr;
    }

    public void setColConvertExpr(String colConvertExpr) {
        this.colConvertExpr = colConvertExpr;
    }

    public Integer getColTemp() {
        return colTemp;
    }

    public void setColTemp(Integer colTemp) {
        this.colTemp = colTemp;
    }

    public String getTopSourceFields() {
        return topSourceFields;
    }

    public void setTopSourceFields(String topSourceFields) {
        this.topSourceFields = topSourceFields;
    }

    public String getRouteSourceFields() {
        return routeSourceFields;
    }

    public void setRouteSourceFields(String routeSourceFields) {
        this.routeSourceFields = routeSourceFields;
    }

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

    public String getFeature() {
        return feature;
    }

    public void setFeature(String feature) {
        this.feature = feature;
    }
}

    
