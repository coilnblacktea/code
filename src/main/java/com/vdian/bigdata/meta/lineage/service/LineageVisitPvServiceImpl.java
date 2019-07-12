package com.vdian.bigdata.meta.lineage.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vdian.bigdata.meta.bo.ResourceColumnBO;
import com.vdian.bigdata.meta.exception.ErrorCode;
import com.vdian.bigdata.meta.exception.MetaException;
import com.vdian.bigdata.meta.lineage.cache.LineageCacheService;
import com.vdian.bigdata.meta.lineage.entity.ColLine;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import com.vdian.bigdata.meta.lineage.parse.LineParser;
import com.vdian.bigdata.meta.lineage.processcycle.LineageProceeCycle;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleEvent;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleListener;
import com.vdian.bigdata.meta.lineage.processcycle.ProcessCycleState;
import com.vdian.bigdata.meta.lineage.util.DateUtil;
import com.vdian.bigdata.meta.lineage.util.SqlResultUtil;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColAccessDO;
import com.vdian.bigdata.meta.meta.domain.lineage.MetaColLineageDO;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColAccessMapper;
import com.vdian.bigdata.meta.meta.mapper.lineage.MetaColLineageMapper;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-09  10:48
 **/

@Service
public class LineageVisitPvServiceImpl implements  LineageVisitPvService, ProcessCycleListener {


    private static final Logger logger = LoggerFactory.getLogger(LineageVisitPvServiceImpl.class);

    @Autowired
    private AdhocScriptFetcher fetcher;

    @Autowired
    private LineParser lineParser;

    @Autowired
    private SqlResultUtil sqlResultUtil;

    @Autowired
    private LineageCacheService lineageCacheService;

    @Autowired
    private MetaColAccessMapper metaColAccessMapper;

    @Autowired
    private MetaColLineageMapper metaColLineageMapper;

    @Autowired
    private MetaLineageDownStreamFetcher downStreamFetcher;

    @Autowired
    private LineageProceeCycle lineageProceeCycle;

    private Map<Long,Long> cache = Maps.newHashMap();

    private Integer message;

    @PostConstruct
    public void init(){
        lineageProceeCycle.addProcessCycleListener(this);
    }

    @Override
    public void countVisitPv(Integer start) {

        //每次计算前，把上一次的数据清理掉
        cache.clear();


        String startTime = new SimpleDateFormat("yyyy-MM-dd").format(DateUtil.addDays(new Date(),-start)) + " 00:00:00";
        String endTime = new SimpleDateFormat("yyyy-MM-dd").format(DateUtil.addDays(new Date(),-1)) + " 59:59:59";


        List<String> adHocScripts = fetcher.fetch(startTime,endTime);

        while(!CollectionUtils.isEmpty(adHocScripts)){
            procesAdhocScripts(adHocScripts);

            updateDb();
            cache.clear();
            //处理完后 得到本次处理的所有字段访问次数  然后更新库
            adHocScripts = fetcher.fetch(startTime,endTime);
        }


        //visitPv计算完成后 需要计算字段的sum_visit_pv
        //字段的sum_visit_pv 的计算是把字段的所有下游字段的visit_pv做累加

        calculateSumPv();

    }

    @Override
    public MetaColAccessDO countVisitPv(String colName) {

        ResourceColumnBO resourceColumnBO = lineageCacheService.getByColumnName(colName);

        if(null == resourceColumnBO){
            throw new MetaException(String.format("can't find col in resource_column by name [%s]",colName), ErrorCode.PARAM_ERROR);
        }

        MetaColLineageDO metaColLineageDO = lineageCacheService.getMetaColByColId(resourceColumnBO.getId());

        if(null == metaColLineageDO){
            throw new MetaException(String.format("can't find col in meta_col_lineage by id[%s]",resourceColumnBO.getId()),ErrorCode.PARAM_ERROR);
        }

        return getAccess(metaColLineageDO);
    }

    /**
     * 计算字段你的sum_pv
     */
    private void calculateSumPv(){
        List<MetaColLineageDO> toProcess = downStreamFetcher.fetch();

        while(!CollectionUtils.isEmpty(toProcess)){
            List<MetaColAccessDO> accessDOS = toProcess.stream().map(this::getAccess).filter(Objects::nonNull).collect(Collectors.toList());
            //这里只更新lineage表中的sum_visit_pv
            updateSumPv(accessDOS);
            toProcess = downStreamFetcher.fetch();
        }

        if(logger.isInfoEnabled()){
            logger.info("calc sum visit pv success");
        }



    }


    /**
     * 更新lineage表中的 字段价值度  access表中不需要此字段了 暂时不更新access表，如果需要，后面更新
     * @param accessDOS
     */
    private void updateSumPv(List<MetaColAccessDO> accessDOS) {
        if(CollectionUtils.isEmpty(accessDOS)){
            return;
        }

        List<MetaColLineageDO> colLineageDOS = accessDOS.stream().map(e ->{
            MetaColLineageDO metaColLineageDO = new MetaColLineageDO();
            metaColLineageDO.setColResourceId(e.getColResourceId());
            metaColLineageDO.setColVisitPv(e.getColVisitPv());
            metaColLineageDO.setColSumVisitPv(e.getColSumVisitPv());
            return metaColLineageDO;
        }).collect(Collectors.toList());

        int updates = metaColLineageMapper.updatePv(colLineageDOS);

        //更新 meta_col_visit
        if(updates != accessDOS.size()){
            logger.error("update sum visit pv , total is [{}] update [{}}",accessDOS.size(),updates);
        }


    }


    /**
     * 计算sum_visit_pv 然后只更新这个字段
     * @param lineageDO
     * @return
     */
    private MetaColAccessDO getAccess(MetaColLineageDO lineageDO){

        Long sumVisitPv = 0L;

        sumVisitPv += calcuRecursive(lineageDO);

        MetaColAccessDO metaColAccessDO = metaColAccessMapper.getByColId(lineageDO.getColResourceId());
        if(null == metaColAccessDO){
            return null;
        }


        metaColAccessDO.setColSumVisitPv(sumVisitPv);


        return metaColAccessDO;
    }

    /**
     * 递归计算字段的价值度
     * @param metaColLineageDO
     */
    private Long calcuRecursive(MetaColLineageDO metaColLineageDO){

        MetaColAccessDO metaColAccessDO = metaColAccessMapper.getByColId(metaColLineageDO.getColResourceId());
        if(null == metaColAccessDO ){
            return 0L;
        }
        //如果metaColineageDo的直接下游为空，那么该字段的价值度等于字段的热度 即sum_visit_pv = visit_pv
        String directDownstreams = metaColLineageDO.getColDirectDownSourceFields();
        if(StringUtils.isEmpty(directDownstreams)){
            return metaColAccessDO.getColVisitPv();
        }

        List<Long> downStreamIds = Arrays.asList(StringUtils.split(directDownstreams,",")).stream().map(Long::valueOf).collect(Collectors.toList());

        List<MetaColLineageDO> colLineageDOS = downStreamIds.stream().map(metaColLineageMapper::getByColId).filter(Objects::nonNull).collect(Collectors.toList());


        return metaColAccessDO.getColVisitPv() + colLineageDOS.stream().mapToLong(this::calcuRecursive).sum();
    }

    /**
     * 处理一批更新一批
     * 防止map里缓存数据过大
     */
    private void updateDb(){
        if(cache.size() == 0){
            return;
        }

        //每次更新先删除，然后再更新数据
        List<Long> toDeleteIds = Lists.newArrayList();

        List<MetaColAccessDO> toInsert = cache.entrySet().stream().map(e ->{
            Long colId = e.getKey();
            MetaColAccessDO colAccessDO = metaColAccessMapper.getByColId(colId);
            if(null == colAccessDO) {
                //还没有
                colAccessDO = new MetaColAccessDO();
                colAccessDO.setColResourceId(colId);
                colAccessDO.setColVisitPv(e.getValue());
                colAccessDO.setColSumVisitPv(e.getValue());
            }else{
                toDeleteIds.add(colId);
                if(colAccessDO.getColVisitPv() != null){
                    colAccessDO.setColVisitPv(colAccessDO.getColVisitPv()+e.getValue());
                    colAccessDO.setColSumVisitPv(colAccessDO.getColSumVisitPv() == null ? colAccessDO.getColVisitPv(): colAccessDO.getColSumVisitPv());
                }else{
                    colAccessDO.setColVisitPv(colAccessDO.getColVisitPv());
                    colAccessDO.setColSumVisitPv(colAccessDO.getColVisitPv());
                }
            }

            return colAccessDO;
        }).collect(Collectors.toList());

        //首先删除 已经存在
        metaColAccessMapper.batchDelete(toDeleteIds);

        metaColAccessMapper.batchInsert(toInsert);
    }

    /**
     *
     * @param adHocScripts
     */
    private void procesAdhocScripts(List<String> adHocScripts){

        adHocScripts.stream().forEach(this::processScript);
    }


    /**
     * 拿到脚本进行解析
     * 解析出来后根据字段名找到相应的字段id
     * 然后对字段的访问次数+1
     * @param script
     */
    private void processScript(String script){
        List<SQLResult> parseResult = Lists.newArrayList();
        try {
            parseResult = lineParser.parse(script);
        }catch(Exception e){
            logger.error("parse [{}] error, message is [{}]",script, ExceptionUtils.getStackTrace(e));
        }

        //如果不是一个纯select语句 返回
        if(!sqlResultUtil.isPureSelect(parseResult)){
            logger.error("script [{}} is not pure select,please check ",script);
            return;
        }


        parseResult.stream().forEach(e ->{
            List<ColLine> colines = e.getColLineList();
            if(CollectionUtils.isEmpty(colines)){
                return;
            }

            colines.stream().forEach(temp ->{
                if(CollectionUtils.isEmpty(temp.getFromNameSet())){
                    return;
                }

                String colName = temp.getFromNameSet().iterator().next();
                ResourceColumnBO columnBO = lineageCacheService.getByColumnName(colName);
                if(null == columnBO){
                    logger.error("col [{}] 在column表中找不到对应的信息，请确认!",colName);
                    return;
                }

                if(cache.containsKey(columnBO.getId())){
                    cache.put(columnBO.getId(),cache.get(columnBO.getId())+1);
                }else{
                    cache.put(columnBO.getId(),1L);
                }
            });
        });

    }

    @Override
    public boolean canHandle(ProcessCycleEvent event) {
        if(event.getState().equals(ProcessCycleState.LINEAGE_ANALYZED)){
            this.message = (Integer) event.getMessage();
            lineageProceeCycle.setState(ProcessCycleState.LINEAGE_PV_COUNTING);
            return true;
        }

        return false;
    }

    @Override
    @Transactional(transactionManager = "metaTransactionManager")
    public void processCycleEvent(ProcessCycleEvent event) {
        if(canHandle(event)){
            countVisitPv(this.message);
        }


    }

    @Override
    public void fireEvent() {
        lineageProceeCycle.fireProcessEvent(new ProcessCycleEvent(ProcessCycleState.LINEAGE_PV_COUNTED,this.message));
    }
}

    
