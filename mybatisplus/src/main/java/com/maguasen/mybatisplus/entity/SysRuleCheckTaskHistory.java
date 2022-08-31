package com.maguasen.mybatisplus.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

/**
 * @Function 检核任务历史
 *
 * @author yyh
 * @date 2022-06-23 14:49:32
 */
@Data
@TableName("sys_rule_check_task_history")
@EqualsAndHashCode(callSuper = true)
public class SysRuleCheckTaskHistory extends Model<SysRuleCheckTaskHistory> {

    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    @TableId
    private Integer id;

    /**
     * 任务名称
     */
    private String taskName;

    /**
     * 检核方案名称
     */
    private String checkSchemeName;

    /**
     * 任务执行状态
     */
    private String taskExecState;

    /**
     * 任务执行结果
     */
    private String taskExecResult;

    /**
     * 任务开始时间
     */
    private LocalDateTime taskExecStartTime;

    /**
     * 任务结束时间
     */
    private LocalDateTime taskExecFinishTime;

    /**
     * 数据来源
     */
    private String dataSource;

    /**
     * 方案描述
     */
    private String schemeDescription;

    /**
     * 检核方案对象
     */
    private String checkSchemeObject;

    /**
     * 执行频率
     */
    private String executionFrequency;

    /**
     * 数据范围
     */
    private String dataScope;

    /**
     * 数据资产个数
     */
    private Integer dataAssetsNum;

    /**
     * 数据表（张）
     */
    private Integer dataTableNum;

    /**
     * 失败处理策略
     */
    private String failureHandlePolicy;

    /**
     * 方案创建时间
     */
    private LocalDateTime schemeCreateTime;

}
