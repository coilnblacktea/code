package com.vdian.bigdata.meta.lineage.entity.lineage;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Date;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-03  10:08
 **/

@ApiModel("血缘关系基础对象")
public class MetaLineageBaseBO {

    @ApiModelProperty("主键id")
    private Long id;

    @ApiModelProperty("创建时间")
    private Date gmtCreate;

    @ApiModelProperty("更新时间")
    private Date gmtModified;

    @ApiModelProperty("字段的资源id 关联meta_resource_column")
    private Long colResourceId;


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

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public Long getColResourceId() {
        return colResourceId;
    }

    public void setColResourceId(Long colResourceId) {
        this.colResourceId = colResourceId;
    }
}

    
