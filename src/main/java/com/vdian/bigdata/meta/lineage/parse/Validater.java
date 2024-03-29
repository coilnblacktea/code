package com.vdian.bigdata.meta.lineage.parse;


import com.vdian.bigdata.meta.lineage.entity.ColLine;
import com.vdian.bigdata.meta.lineage.entity.SQLResult;
import com.vdian.bigdata.meta.lineage.exception.VolidateException;
import com.vdian.bigdata.meta.lineage.service.LineageMetaService;
import com.vdian.bigdata.meta.lineage.util.Check;
import com.vdian.bigdata.meta.lineage.util.NumberUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.Map.Entry;

@Service
public class Validater {
	
    private Map<String/*table*/, List<String/*column*/>> dbMap = new HashMap<String, List<String>>();

    @Autowired
	private LineageMetaService metaService;
    /**
     *
     * @param srList
     */
   public void validate(List<SQLResult> srList) {
	   
	   for (SQLResult sr : srList) {
		   Set<String> inputTables = sr.getInputTables();
		   Set<String> outputTables = sr.getOutputTables();
		   List<ColLine> colLineList = sr.getColLineList();
		   
		   if (Check.isEmpty(outputTables)) {
	    		throw new VolidateException("no output table");
			} 
	    	if (Check.isEmpty(inputTables)) {
	    		throw new VolidateException("no input table");
			} 
	    	
	    	for (String table : inputTables) {
				if (! metaService.isTableExsits(table)) {
					throw new VolidateException("input table not exist: " + table.toLowerCase()); 
				}
			}
	    	
	       	for (String table : outputTables) {
				if (!metaService.isTableExsits(table)) {
					throw new VolidateException("out table not exist: " + table.toLowerCase()); 
				}
	    	}
	    	
	    	
	    	Map<String, List<ColLine>> map = new HashMap<String, List<ColLine>>();
	    	for (ColLine colLine : colLineList) {
	    		List<ColLine> list = map.get(colLine.getToTable());
	    		if (Check.isEmpty(list)) {
	    			list = new ArrayList<ColLine>();
	    			map.put(colLine.getToTable(), list);
	    		}
	    		list.add(colLine);
	    		
				if (Check.isEmpty(colLine.getToName())) {
					throw new VolidateException(" no match output column:" + colLine);
				}
				if (!outputTables.contains(colLine.getToTable())) {
					throw new VolidateException(" no output table:" + colLine);
				}
				checkInputTableInfo(colLine);
			}
	    	checkOutputTableInfo(outputTables, map);
	   }
	   
    	
	}
   

	private void checkOutputTableInfo(Set<String> outputTables, Map<String, List<ColLine>> map) {
		Map<String, List<String>> dbMap  = new HashMap<String, List<String>>();
    	for (String table : outputTables) {
    		List<String> list = metaService.getByTable(table);
    		dbMap.put(table, list);
		}
		
		for (Entry<String, List<ColLine>> entry : map.entrySet()) {
			String table = entry.getKey();
			List<ColLine> pList = entry.getValue();
			List<String> dList = dbMap.get(table);
			if (Check.isEmpty(dList)) {
				throw new VolidateException(" meta data table '"+table+"'" + " has no column.");
			}
			if (pList.size() != dList.size()) {
				throw new VolidateException(" column number/types are different '"+table+"': Table insclause-0 has "+dList.size()+" columns, but query has "+pList.size()+" columns.");
			}
		}
	}

    /**
     *
     * @param colLine
     */
	private void checkInputTableInfo(ColLine colLine) {
		Set<String> fromNameSet = colLine.getFromNameSet();
		for (String fromName : fromNameSet) {
			if (NumberUtil.isNumeric(fromName)) { //0.01
				continue;
			}
			
			int lastIndexOf = fromName.lastIndexOf('.');
			String column = fromName.substring(lastIndexOf+1);
			String table = fromName.substring(0, lastIndexOf);
			List<String> list = initAndGet(table);
			boolean tableNoCol = true;
			if (Check.notEmpty(list)) {
				for (String string : list) {
					if (column.equalsIgnoreCase(string)) {
						tableNoCol = false;
					}
				}
			}
			if (tableNoCol) {
				throw new VolidateException(" input table " + table + " no this column:" + column);
			}
		}
	}
	
	private List<String> initAndGet(String table){
		List<String> list2 = dbMap.get(table);
		if (Check.isEmpty(list2)) {
			List<String> list = metaService.getByTable(table);
			dbMap.put(table, list);
			list2 = list;
		}
		return list2;
	}
}
