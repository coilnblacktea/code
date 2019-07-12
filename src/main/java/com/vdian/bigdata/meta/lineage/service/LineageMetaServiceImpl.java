package com.vdian.bigdata.meta.lineage.service;

import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.bo.ResourceBO;
import com.vdian.bigdata.meta.bo.ResourceColumnBO;
import com.vdian.bigdata.meta.bo.ResourceGidBO;
import com.vdian.bigdata.meta.client.domain.ResourceColumnDO;
import com.vdian.bigdata.meta.client.mapper.ResourceColumnMapper;
import com.vdian.bigdata.meta.enums.ResourceTypeEnum;
import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.service.ResourceService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-06-28  14:39
 **/

@Service
public class LineageMetaServiceImpl implements  LineageMetaService {

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private ResourceColumnMapper columnMapper;

    private static final String HIVE_GUOYU = "guoyu";

    @Override
    public List<ResourceColumnBO> getColumnDetailsByTable(String tableName) {
        validateTable(tableName);

        ResourceGidBO resourceGidBO = constructQuery(tableName);

        return resourceService.queryResourceColumn(null,resourceGidBO,null);

    }

    @Override
    public List<String> getByTable(String tableName) {

        List<ResourceColumnBO> columns = getColumnDetailsByTable(tableName);
        if(CollectionUtils.isEmpty(columns)){
            return Lists.newArrayList();
        }

        return columns.stream().map(ResourceColumnBO::getName).collect(Collectors.toList());

    }

    @Override
    public boolean isTableExsits(String tableName) {
        validateTable(tableName);

        ResourceGidBO resourceGidBO = constructQuery(tableName);

        ResourceBO resourceBO = resourceService.checkResourceExist(resourceGidBO);

        return resourceBO != null;

    }

    @Override
    public ResourceColumnBO getByColumnId(Long columnId) {
        ResourceColumnDO columnDO =  columnMapper.getBriefInfoByColId(columnId);

        ResourceColumnBO result = new ResourceColumnBO();
        if(null == columnDO || columnDO.getId() == null){
            return result;
        }

        BeanUtils.copyProperties(columnDO,result);

        return result;
    }

    /**
     *
     * @param tableName
     * @return
     */
    private ResourceGidBO constructQuery(String tableName){
        String[] splits = StringUtils.split(tableName,"\\.");
        ResourceGidBO resourceGidBO = new ResourceGidBO();
        resourceGidBO.setDbName(splits[0]);
        resourceGidBO.setTableName(splits[1]);
        resourceGidBO.setResourceType(ResourceTypeEnum.HIVE);
        resourceGidBO.setCluster(HIVE_GUOYU);
        return resourceGidBO;
    }


    /**
     * 检查表名是否合法  表名为db.tableName形式
     * @param tableName
     */
    private void validateTable(String tableName){
        if(StringUtils.isEmpty(tableName)){
            throw new MetaException("表名不允许为空或者空字符串!", ErrorCode.PARAM_ERROR);
        }

        String[] splits = StringUtils.split(tableName,"\\.");
        if(splits.length != 2){
            throw new MetaException(String.format("表名[%s]非法，正确的格式是 db.tableName形式",tableName),ErrorCode.PARAM_ERROR);
        }
    }
}

    
