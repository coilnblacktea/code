package com.vdian.bigdata.meta.lineage.cache;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.vdian.bigdata.meta.bo.ResourceColumnBO;
import com.vdian.bigdata.meta.lineage.service.LineageMetaService;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-01  10:43
 **/

@Service
public class LineageCacheService {


    private static final Logger logger = LoggerFactory.getLogger(LineageCacheService.class);


    @Autowired
    private LineageMetaService lineageMetaService;

    @Autowired
    private MetaColLineageMapper metaColLineageMapper;


    /**
     * 表关联字段的缓存关系
     */
    public LoadingCache<String, List<ResourceColumnBO>> tableResources = CacheBuilder.newBuilder()
                .maximumSize(7000)
                .expireAfterWrite(10L, TimeUnit.HOURS)
                .initialCapacity(1000)
                .softValues()
                .recordStats()
                .build(
                        new CacheLoader<String, List<ResourceColumnBO>>() {
                            @Override
                            public List<ResourceColumnBO> load(String tableName) throws Exception {
                                return getColumnsByTableName(tableName);
                            }
                        }
                );


    /**
     * 字段id与字段资源的映射
     */
    public LoadingCache<Long,ResourceColumnBO> columnIdResources = CacheBuilder.newBuilder()
            .maximumSize(50000)
            .expireAfterWrite(10L,TimeUnit.HOURS)
            .initialCapacity(10000)
            .softValues()
            .recordStats()
            .build(
                    new CacheLoader<Long, ResourceColumnBO>() {
                        @Override
                        public ResourceColumnBO load(Long columnId) throws Exception {
                            return getByColumnById(columnId);
                        }
                    }
            );



    public LoadingCache<Long, MetaColLineageDO> columnMetaLineageResources = CacheBuilder.newBuilder()
            .maximumSize(50000)
            .expireAfterWrite(10L,TimeUnit.HOURS)
            .initialCapacity(10000)
            .softValues()
            .recordStats()
            .build(new CacheLoader<Long, MetaColLineageDO>() {
                @Override
                public MetaColLineageDO load(Long aLong) throws Exception {
                    return getColLineageByColId(aLong);
                }
            });


    /**
     * 字段名与字段的映射
     */
    public LoadingCache<String,ResourceColumnBO> columnNameResources = CacheBuilder.newBuilder()
            .maximumSize(50000)
            .expireAfterWrite(10L,TimeUnit.HOURS)
            .initialCapacity(10000)
            .softValues()
            .recordStats()
            .build(
                    new CacheLoader<String, ResourceColumnBO>() {
                        @Override
                        public ResourceColumnBO load(String columnName) throws Exception {
                            return getByCoumnName(columnName);
                        }
                    }
                    );




    /**
     * 获取 字段名缓存统计信息
     * @return
     */
    public String getColumnNameCacheStats(){

      return getByCache(columnNameResources);
    }

    /**
     *
     * @param colId
     * @return
     */
    private MetaColLineageDO getColLineageByColId(Long colId){
        return metaColLineageMapper.getCacheInfoByColId(colId);
    }

    private String getByCache(LoadingCache loadingCache){
        CacheStats stats =  loadingCache.stats();
        StringBuilder sb = new StringBuilder();
        sb.append("缓存load值次数[").append(stats.loadCount()).append("]").append("   ")
                .append("缓存load成功次数[").append(stats.loadSuccessCount()).append("]").append("   ")
                .append("缓存load异常次数[").append(stats.loadExceptionCount()).append("]").append("   ")
                .append("缓存load出现异常比率[").append(stats.loadExceptionRate()).append("]").append("   ")
                .append("缓存load值加载总时间[").append(getMille(stats.totalLoadTime())).append("毫秒]").append("   ")
                .append("查询缓存次数[").append(stats.requestCount()).append("]").append("   ")
                .append("命中缓存次数[").append(stats.hitCount()).append("]").append("   ")
                .append("查询缓存未命中次数[").append(stats.missCount()).append("]").append("   ")
                .append("查询缓存未命中率[").append(stats.missRate()).append("]").append("   ")
                .append("加载新值得平均时间[").append(stats.averageLoadPenalty()/1000000).append("毫秒]").append("   ")
                .append("缓存被回收总数[").append(stats.evictionCount()).append("]");

        return sb.toString();
    }

    /**
     *
     * @param mills
     * @return
     */
    private Long getMille(Long nanos){
        return nanos/1000000;
    }

    /**
     * 获取字段id缓存统计信息
     * @return
     */
    public String getColumnIdCacheStats(){
        return getByCache(columnIdResources);
    }


    public String getCoolumnLineageCacheStats(){
        return getByCache(columnMetaLineageResources);
    }

    /**
     *
     * @return
     */
    public String getTableCacheStats(){
        return getByCache(tableResources);
    }


    public MetaColLineageDO getMetaColByColId(Long colId){
        try {
            return columnMetaLineageResources.get(colId);
        } catch (ExecutionException e) {
            logger.warn("cache service getMetaColByColId,key:[{}], error [{}]",colId, ExceptionUtils.getStackTrace(e));
        }

        return null;
    }

    private ResourceColumnBO getByCoumnName(String columnName){
        if(StringUtils.isEmpty(columnName)){
            return null;
        }

        String[]splits = StringUtils.split(columnName,"\\.");
        if(splits.length != 3){
            return null;
        }

        String tableName = Joiner.on(".").join(splits[0],splits[1]);

        try {
            List<ResourceColumnBO> columns = getByTable(tableName);
            if(CollectionUtils.isEmpty(columns)){
                return null;
            }

            for(ResourceColumnBO bo: columns){
                if(bo.getName().equals(splits[2])){
                    return bo;
                }
            }

        }catch(ExecutionException e){
            logger.warn("cache service getByColumnName,key:[{}], error [{}]",columnName, ExceptionUtils.getStackTrace(e));
        }

        return null;
    }


    private ResourceColumnBO getByColumnById(Long columnId){
        return lineageMetaService.getByColumnId(columnId);
    }

    private List<ResourceColumnBO> getColumnsByTableName(String tableName){

        return lineageMetaService.getColumnDetailsByTable(tableName);
    }


    /**
     *
     * @param columnId
     * @return
     * @throws ExecutionException
     */
    public ResourceColumnBO getByColumnId(Long columnId)throws ExecutionException{
        return columnIdResources.get(columnId);
    }


    /**
     * 根据表名批量查询
     * @param colNames
     * @return
     */
    public List<ResourceColumnBO> batchGetByColumnNames(List<String> colNames){

        if(CollectionUtils.isEmpty(colNames)){
            return Lists.newArrayList();
        }

        return colNames.stream().map(e ->{
           return getByColumnName(e);
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }
    /**
     * 批量查询
     * @param columnIds
     * @return
     */
    public List<ResourceColumnBO> batchGetByColumnIds(List<Long> columnIds){
        if(CollectionUtils.isEmpty(columnIds)){
            return Lists.newArrayList();
        }


       return columnIds.stream().map(this::getByColumnById).collect(Collectors.toList());
    }

    /**
     * 根据字段名查询字段详情  （db.tableName.colName）
     * @param columnName
     * @return
     * @throws ExecutionException
     */
    public ResourceColumnBO getByColumnName(String columnName) {
        try {
            return columnNameResources.get(columnName);
        } catch (Exception e) {
            logger.warn("cache service getByColumnName,key:[{}],return null",e);
        }
        return null;
    }
    /**
     *
     * @param tableName
     * @return
     * @throws ExecutionException
     */
    public List<ResourceColumnBO> getByTable(String tableName) throws ExecutionException {
        return tableResources.get(tableName);
    }


}

    
