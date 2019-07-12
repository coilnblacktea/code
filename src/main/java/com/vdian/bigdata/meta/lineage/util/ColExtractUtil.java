package com.vdian.bigdata.meta.lineage.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-02  09:25
 **/

@Service
public class ColExtractUtil {


    private static final Logger logger = LoggerFactory.getLogger(ColExtractUtil.class);

    private String rule = "[a-zA-Z0-9_]*\\.[a-zA-Z0-9_]*\\.[a-zA-Z0-9_]*";

    private Pattern pattern;

    @PostConstruct
    public void init(){
        pattern = Pattern.compile(rule);
    }
    /**
     * 缓存表达式以及从表达式中解析出的字段
     */
    private LoadingCache<String, List<String>>colsCache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .initialCapacity(10)
                .softValues()
                .recordStats()
                .build(
                        new CacheLoader<String, List<String>>() {
                            @Override
                            public List<String> load(String expr) throws Exception {
                                return extractColsFromExpr(expr);
                            }
                        }
                );


    /**
     * 从where或join表达式中抽取出字段
     * @param expr
     * @return
     */
    private List<String> extractColsFromExpr(String expr){

        Matcher matcher = pattern.matcher(expr);
        List<String> result = Lists.newArrayList();

        while(matcher.find()){
            result.add(matcher.group());
        }

        return result;
    }


    /**
     *
     * @param expr
     * @return
     */
    public  List<String> getColsFromExpr(String expr){
        try {
            return colsCache.get(expr);
        } catch (ExecutionException e) {
            logger.warn("extract cols from [{}] failed [{}]",expr, ExceptionUtils.getStackTrace(e));
        }

        return Lists.newArrayList();
    }


    public boolean isWhereClause(String expr){
        if(StringUtils.isEmpty(expr)){
            return false;
        }

        return expr.trim().startsWith("WHERE:");
    }

}

    
