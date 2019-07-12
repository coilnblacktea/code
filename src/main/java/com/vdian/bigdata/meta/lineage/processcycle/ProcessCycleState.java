package com.vdian.bigdata.meta.lineage.processcycle;

/**
 * @author: lhy
 * @description
 * @created: 2019-07-11  09:41
 **/


public enum ProcessCycleState {

    /**
     * 流程还未开始
     */
    NEW,
    /**
     * 解析脚本中
     */
    SCRIPT_ANALYZING,

    /**
     * 脚本解析完成
     */
    SCRIPT_ANALYZED,

    /**
     * 血缘关系溯源中
     */
    LINEAGE_ANALYZING,

    /**
     * 血缘关系溯源完成
     */
    LINEAGE_ANALYZED,

    /**
     * 计算字段的pv和sum_visit_pv中
     */

    LINEAGE_PV_COUNTING,

    /**
     * 计算字段的pv和sum_visit_pv完成
     */
    LINEAGE_PV_COUNTED,

    /**
     * 计算字段的相似度
     */
    LINEAGE_SIMILARITY_PROCESSING,

    /**
     * 字段相似度计算完成
     */
    LINEAGE_SIMILARITY_PROCESSED,

    /**
     * 一次计算的终结
     */
    LINEAGE_PROCESS_END;


}
