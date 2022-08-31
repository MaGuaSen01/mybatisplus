package com.maguasen.mybatisplus.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataProcess {
    public static void main(String[] args) {

    }

    public static ArrayList<String> SQL_process(String result_database, String result_table, List<HashMap<String,String>> rule_args) {
        ArrayList<String> SQL_list = new ArrayList<>();   //建表时hivetime是否添加(入库时间)
        String init = String.format("  create table if not exists %s.%s (hiveid int,value string,rid string,ridname string,`hivetable` string,`hivedatabase` string,department string) ", result_database, result_table);
        SQL_list.add(init);

        for (Map<String, String> rule_arg : rule_args) {
            String str = "";
            String SQL = rule_arg.get("SQL");
            String department = rule_arg.get("department");
            String database = rule_arg.get("database");
            String table = rule_arg.get("table");
            String col = rule_arg.get("col");   //被检核字段名
            String rid = rule_arg.get("rid");
            String ridname = rule_arg.get("ridname");
            String time_down = rule_arg.get("time_down");
            String time_up = rule_arg.get("time_up");
            String rule_type = rule_arg.get("rule_type");  //合格数据,不合格数据

            String insert = String.format(" insert into table %s.%s ", result_database, result_table);
            String bad = String.format(" select t1.hiveid,t1.value,'%s' as rid,'%s' as ridname,'%s' as `hivetable`,'%s' as `hivedatabase`,'%s' as department from (select  hiveid,%s as value from (select * from %s.%s where time>='%s' and time <='%s')t0 %s)t1 ", rid, ridname, table, database, department, col, database, table, time_down, time_up, SQL);   //①增加字段原始值②时间可以为current_date():2022-08-15
            String good = String.format(" select t1.hiveid,t1.value,'%s' as rid,'%s' as ridname,'%s' as `hivetable`,'%s' as `hivedatabase`,'%s' as department from (select hiveid from %s.%s where hiveid not in (select hiveid,%s as value from (select * from %s.%s where time>='%s' and time <='%s')t0 %s))t1 ", rid, ridname, table, database, department, database, table, col, database, table, time_down, time_up, SQL);

            System.out.println(rule_args.size());

            if (rule_type == "合格数据") {
                str = insert + good;
            }
            if (rule_type == "不合格数据") {
                str = insert + bad;
            }
            //  str = str + ";";   dolphin的SQL节点不需要分号结尾
            SQL_list.add(str);
            System.out.println("该字符串为:" + str);
        }

        return SQL_list;
    }
}
