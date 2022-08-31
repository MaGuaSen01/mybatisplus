package com.maguasen.mybatisplus.hive;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

import java.security.PublicKey;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class RestfulTest {
	public static void main(String[] args) {
		/**
		 * dolphin初始化参数
		 */
		//String http="http://172.16.253.111:12345";
		//String token="1138260c339351300cf25c626504e7fa";
		String http="http://10.0.7.59:12345";
		String token="3fdeafff1b25bf7947bfe24c6aadd471";
		String projectName="data-quality-v2.0";

		//String tenantCode="dolphinscheduler";  //租户
		String tenantCode="dm租户";  //租户
		Integer datasourceId=6;
		String worker_group="default";
		String cron="0 0 1 * * ? *";
		String result_database="xutest";
		String result_table1="column_test";
		String result_table2="table_test";
		String schemeName="方案一";

		/**
		 * 字段规则处理参数初始化
		 */
		ArrayList<HashMap<String, String>> SQL_coltest  = new ArrayList<HashMap<String, String>>();
		HashMap<String, String> test1 = new HashMap<String, String>();
		test1.put("SQL","where age>70");
		test1.put("department","AAA");
		test1.put("database","xutest");
		test1.put("table","okok");
		test1.put("rid","33");
		test1.put("ridname","33规则");
		test1.put("time_down","2020-7-19");
		test1.put("time_up","2022-7-20");
		test1.put("rule_type","合格数据");
		SQL_coltest.add(test1);

		HashMap<String, String> test2 = new HashMap<String, String>();
		test2.put("SQL","where age<50");
		test2.put("department","BBB");
		test2.put("database","xutest");
		test2.put("table","okok");
		test2.put("rid","44");
		test2.put("ridname","44规则");
		test2.put("time_down","2020-7-19");
		test2.put("time_up","2022-7-20");
		test2.put("rule_type","不合格数据");
		SQL_coltest.add(test2);


		/**
		 * 表规则处理参数初始化
		 */
		ArrayList<HashMap<String, String>> SQL_tabletest  = new ArrayList<HashMap<String, String>>();
		HashMap<String, String> test3 = new HashMap<String, String>();
		test3.put("SQL","select *,age as value from xutest.okok where age>80");
		test3.put("department","AAA");
		test3.put("database","xutest");
		test3.put("table","okok");
		test3.put("rid","55");
		test3.put("ridname","55规则");
		test3.put("time_down","2020-7-19");
		test3.put("time_up","2022-7-20");
		test3.put("column_name","age");
		SQL_tabletest.add(test3);

		HashMap<String, String> test4 = new HashMap<String, String>();
		test4.put("SQL","select *,age as value from xutest.okok where age<50");
		test4.put("department","BBB");
		test4.put("database","xutest");
		test4.put("table","okok");
		test4.put("rid","66");
		test4.put("ridname","66规则");
		test4.put("time_down","2020-7-19");
		test4.put("time_up","2022-7-20");
		test4.put("column_name","age");
		SQL_tabletest.add(test4);

		/**
		 * 调用
		 */
//        String workflow_id="400";
//        workflow_id = dolphin_col_create(http, token, projectName, tenantCode, datasourceId, worker_group, schemeName,result_database, result_table1, SQL_coltest);//字段规则任务创建
//        System.out.println(workflow_id);
//        workflow_id = dolphin_table_create(http,token,projectName,tenantCode,datasourceId,worker_group,schemeName,result_database,result_table2,SQL_coltest);  //表任务创建
////
//        set_time(http,token,projectName,workflow_id,cron);      //设置定时
//        process_instance(http,token,projectName,workflow_id);   //立即执行
//        String status = get_status(http, token, projectName,workflow_id);   //根据工作流ID获取工作流实例执行状态,返回值为该工作流的全部实例,jsonArray
//        System.out.println(status);
//
//        workflow_delect(http,token,projectName,workflow_id);   //工作流删除
//       workflow_id =dolphin_col_revise(http,token,workflow_id,projectName,tenantCode,datasourceId,worker_group,schemeName,result_database,result_table1,SQL_coltest); //字段工作流修改
//        System.out.println(workflow_id);
//        workflow_id =dolphin_table_revise(http,token,workflow_id,projectName,tenantCode,datasourceId,worker_group,schemeName,result_database,result_table1,SQL_coltest); //表工作流修改
//
//        String log = get_log(http, token, projectName, "taskInstanceId");   //根据实例id获取实例日志
//
//        getInstance_status(http,token,"data-quality-v4.0","119465");   //获取任务实例状态

		String status = get_status(http, token, "PROD-MULT" ,"307" , null , null ,null , 10 , 1);   //获取工作流执行状态
		System.err.println(JSONUtil.parseArray(status));

	}



	/**
	 * 字段任务新增
	 * @param http
	 * @param token
	 * @param projectName
	 * @param tenantCode
	 * @param datasourceId
	 * @param worker_group
	 * @param result_database
	 * @param result_table
	 * @param SQL_col
	 * @return
	 */
	public static String  dolphin_col_create(String http,String token,String projectName,String tenantCode,Integer datasourceId,String worker_group,String schemeName,String result_database,String result_table,ArrayList<HashMap<String, String>> SQL_col){
		List<Integer> taskid_list=new ArrayList<>();
		List<Integer> nodeid_list=new ArrayList<>();
		SimpleDateFormat timeformat = new SimpleDateFormat("yyyy-MM-dd");
		String time = timeformat.format(new Date());
		int rand=RandomUtil.getRandom().nextInt(1000, 9999);
		String workflow_name=schemeName+"/"+time+"/"+rand;    //工作流名为包含方案名

		System.out.println(SQL_col);

		String Accept="application/json";

		ArrayList<String> SQL_result1 = HiveSQL.cloumn_rule(result_database, result_table, SQL_col);

		ArrayList<String> rule_list = HiveSQL.get_rule(SQL_col);

		System.out.println(SQL_result1);

		int size = SQL_result1.size();
		int task_id=98000;
		int node_id=0;

		for (int i = 0; i < size; i++) {
			task_id=task_id+1;
			System.out.println(task_id);  //测试
			taskid_list.add(task_id);
		}

		for (int i = 0; i < size; i++) {
			node_id=node_id+1;
			System.out.println(node_id);  //测试
			nodeid_list.add(node_id);
		}
		creat_project(http,Accept,token,projectName);   //1.创建project
		creat_workflow(http, Accept, token, workflow_name,projectName,taskid_list,nodeid_list,SQL_result1,tenantCode,datasourceId,worker_group,rule_list);
		String workflow_id = get_workflowid(http,Accept,token,projectName,workflow_name);
		workflow_online(http,Accept,token,workflow_id,projectName,"1");  //代表上线
		//  System.out.println(workflow_id);
		return workflow_id;
	}


	/**
	 * 表任务新增
	 * @param http
	 * @param token
	 * @param projectName
	 * @param tenantCode
	 * @param datasourceId
	 * @param worker_group
	 * @param result_database
	 * @param result_table
	 * @param SQL_table
	 * @return
	 */
	public static String  dolphin_table_create(String http,String token,String projectName,String tenantCode,Integer datasourceId,String worker_group,String schemeName,String result_database,String result_table,ArrayList<HashMap<String, String>> SQL_table){
		List<Integer> taskid_list=new ArrayList<>();
		List<Integer> nodeid_list=new ArrayList<>();
		SimpleDateFormat timeformat = new SimpleDateFormat("yyyy-MM-dd");
		String time = timeformat.format(new Date());
		int rand=RandomUtil.getRandom().nextInt(1000, 9999);
		String workflow_name=schemeName+"/"+time+"/"+rand;

		System.out.println(SQL_table);

		String Accept="application/json";


		ArrayList<String> SQL_result2 = HiveSQL.table_rule(result_database, result_table, SQL_table);

		ArrayList<String> rule_list = HiveSQL.get_rule(SQL_table);

		System.out.println(SQL_result2);

		int size = SQL_result2.size();
		int task_id=98000;
		int node_id=0;

		for (int i = 0; i < size; i++) {
			task_id=task_id+1;
			System.out.println(task_id);  //测试
			taskid_list.add(task_id);
		}

		for (int i = 0; i < size; i++) {
			node_id=node_id+1;
			System.out.println(node_id);  //测试
			nodeid_list.add(node_id);
		}

		creat_project(http,Accept,token,projectName);   //1.创建project
		creat_workflow(http, Accept, token, workflow_name,projectName,taskid_list,nodeid_list,SQL_result2,tenantCode,datasourceId,worker_group,rule_list);
		String workflow_id = get_workflowid(http,Accept,token,projectName,workflow_name);
		workflow_online(http,Accept,token,workflow_id,projectName,"1");  //代表上线
		return workflow_id;
	}


	public static void set_time(String http,String token,String projectName,String workflow_id,String cron){
		String Accept="application/json";
		time_set(http, Accept, token,workflow_id,projectName,cron);
		String timesetId = getTimesetId(http, Accept, token, projectName,workflow_id);
		//  System.out.println(timesetId);
		timeset_online(http,Accept,token,projectName,timesetId);
	}


	public static void process_instance(String http,String token,String projectName,String workflow_id){
		System.out.println("--------------------------------post请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("processDefinitionId", workflow_id);//表单参数,body
		getParamMaps.put("scheduleTime","");
		getParamMaps.put("failureStrategy","END");
		getParamMaps.put("warningType","NONE");
		getParamMaps.put("warningGroupId","0");
		getParamMaps.put("execType","");
		getParamMaps.put("startNodeList","");
		getParamMaps.put("taskDependType","TASK_POST");
		getParamMaps.put("runMode","RUN_MODE_SERIAL");
		getParamMaps.put("processInstancePriority","MEDIUM");
		getParamMaps.put("receivers","");
		getParamMaps.put("receiversCc","");
		getParamMaps.put("workerGroup","default");

		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/"+projectName+"/executors/start-process-instance")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
	}


	/**
	 * 状态查询
	 * @param http
	 * @param token
	 * @param projectName
	 * @param workflowId 工作流ID
	 * @param startDate 开始时间
	 * @param endDate  结束时间
	 * @param stateType 状态
	 * @param pageSize 每页条数
	 * @param pageNO    当前页
	 * @return
	 */
	public static String get_status(String http,String token,String projectName,String workflowId , String startDate , String endDate , String stateType , Integer pageSize , Integer pageNO){
		log.info("--------------------dolphinscheduler--get请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("processDefinitionId",workflowId);
		getParamMaps.put("startDate",startDate);
		getParamMaps.put("endDate",endDate);
		getParamMaps.put("stateType",stateType);
		getParamMaps.put("pageSize",pageSize != null ? pageSize : 10);
		getParamMaps.put("pageNo",pageNO != null ? pageNO :1);

		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/projects/"+projectName+"/instance/list-paging")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		log.info("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
//        log.info(jsonobj);   //返回数据
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			Object data = body.get("data");
			String s = com.alibaba.fastjson.JSONObject.toJSONString(data);

//            JSONObject dataobj = new JSONObject(data);
//            Object totalList = dataobj.get("totalList");
//            JSONArray List = new JSONArray(totalList);
//            JSONObject entries = new JSONObject(List.get(0));
//            String state = entries.get("state").toString();     // SUCCESS   FAILURE  RUNNING_EXEUTION
			return s;
		}
		return "0";
	}




	public static String getInstance_status(String http,String token,String projectName,String processInstanceId){   //任务实例状态
		System.out.println("--------------------------------get请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("pageSize",100);//表单参数,body
		getParamMaps.put("pageNo",1);
		getParamMaps.put("processInstanceId",processInstanceId);
		//   getParamMaps.put("startDate","2022-07-07 00:00:00");
		//  getParamMaps.put("endDate","2022-07-08 00:00:00");

		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/projects/"+projectName+"/task-instance/list-paging")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			JSONObject data = new JSONObject(body.get("data"));
			JSONArray dataobj = new JSONArray(data.get("totalList"));
			return dataobj.toString();
		}
		return "0";
	}



	public static String get_log(String http,String token,String projectName,String taskInstanceId){
		System.out.println("--------------------------------get请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("taskInstanceId",taskInstanceId);//实例ID
		getParamMaps.put("skipLineNum",0);
		getParamMaps.put("limit",10000);

		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/log/detail")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			Object data = body.get("data");
			JSONObject dataobj = new JSONObject(data);
			return dataobj.toString();
		}
		return "0";
	}


	public static void workflow_delect(String http,String token,String projectName,String workflow_id){
		String Accept="application/json";
		workflow_online(http,Accept,token,workflow_id,projectName,"0");  //0代表下线

		System.out.println("--------------------------------get请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("processDefinitionId",workflow_id);//表单参数,body

		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/projects/"+projectName+"/process/delete")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
	}


	public static String  dolphin_col_revise(String http,String token,String pre_workflowId,String projectName,String tenantCode,Integer datasourceId,String worker_group,String schemeName,String result_database,String result_table,ArrayList<HashMap<String, String>> SQL_col){
		workflow_delect(http,token,projectName,pre_workflowId);    //根据先前工作流ID删除

		List<Integer> taskid_list=new ArrayList<>();               //重新创建
		List<Integer> nodeid_list=new ArrayList<>();
		SimpleDateFormat timeformat = new SimpleDateFormat("yyyy-MM-dd");
		String time = timeformat.format(new Date());
		int rand=RandomUtil.getRandom().nextInt(1000, 9999);
		String workflow_name=schemeName+"/"+time+"/"+rand;

		System.out.println(SQL_col);

		String Accept="application/json";

		ArrayList<String> SQL_result1 = HiveSQL.cloumn_rule(result_database, result_table, SQL_col);

		System.out.println(SQL_result1);

		ArrayList<String> rule_list = HiveSQL.get_rule(SQL_col);

		int size = SQL_result1.size();
		int task_id=98000;
		int node_id=0;

		for (int i = 0; i < size; i++) {
			task_id=task_id+1;
			System.out.println(task_id);  //测试
			taskid_list.add(task_id);
		}

		for (int i = 0; i < size; i++) {
			node_id=node_id+1;
			System.out.println(node_id);  //测试
			nodeid_list.add(node_id);
		}
		creat_project(http,Accept,token,projectName);   //1.创建project
		creat_workflow(http, Accept, token, workflow_name,projectName,taskid_list,nodeid_list,SQL_result1,tenantCode,datasourceId,worker_group,rule_list);
		String workflow_id = get_workflowid(http,Accept,token,projectName,workflow_name);
		workflow_online(http,Accept,token,workflow_id,projectName,"1");  //代表上线
		//  System.out.println(workflow_id);
		return workflow_id;
	}


	public static String  dolphin_table_revise(String http,String token,String pre_workflowId,String projectName,String tenantCode,Integer datasourceId,String worker_group,String schemeName,String result_database,String result_table,ArrayList<HashMap<String, String>> SQL_table){
		workflow_delect(http,token,projectName,pre_workflowId);    //根据先前工作流ID删除

		List<Integer> taskid_list=new ArrayList<>();
		List<Integer> nodeid_list=new ArrayList<>();
		SimpleDateFormat timeformat = new SimpleDateFormat("yyyy-MM-dd");
		String time = timeformat.format(new Date());
		int rand=RandomUtil.getRandom().nextInt(1000, 9999);
		String workflow_name=schemeName+"/"+time+"/"+rand;

		System.out.println(SQL_table);

		String Accept="application/json";

		ArrayList<String> SQL_result2 = HiveSQL.table_rule(result_database, result_table, SQL_table);

		System.out.println(SQL_result2);

		ArrayList<String> rule_list = HiveSQL.get_rule(SQL_table);

		int size = SQL_result2.size();
		int task_id=98000;
		int node_id=0;

		for (int i = 0; i < size; i++) {
			task_id=task_id+1;
			System.out.println(task_id);  //测试
			taskid_list.add(task_id);
		}

		for (int i = 0; i < size; i++) {
			node_id=node_id+1;
			System.out.println(node_id);  //测试
			nodeid_list.add(node_id);
		}

		creat_project(http,Accept,token,projectName);   //1.创建project
		creat_workflow(http, Accept, token, workflow_name,projectName,taskid_list,nodeid_list,SQL_result2,tenantCode,datasourceId,worker_group,rule_list);
		String workflow_id = get_workflowid(http,Accept,token,projectName,workflow_name);
		workflow_online(http,Accept,token,workflow_id,projectName,"1");  //代表上线
		return workflow_id;
	}




	/**  dolphinscheduler 2.X  关于自动生成流程的相关restful api
	 * ①web端token输入,以下操作header都要带上token
	 * ②提前手动或调用post接口创建项目,项目名称固定,比如hive-quality
	 * ③get获取到名字为dataquality的projectid
	 * ④post创建任务节点(一个节点对应一个post) ==>需要projectid -->响应体里获取到taskid
	 * ⑤post创建工作流  ==>需要projectid,taskid -->响应体里获取到workflowid
	 * ⑥post工作流上线 ==>需要projectid,workflowid
	 * ⑦post工作流定时==>需要projectid,workflowid  -->响应体中获得定时任务ID
	 * ⑧post定时任务上线 ==>需要projectid,定时任务id
	 */

	/**  dolphinscheduler 1.X  关于自动生成定时流程的相关restful api   1.X对id不看重,projectname即可
	 * ①web端token输入,以下操作header都要带上token
	 * ②提前手动或调用post接口创建项目,项目名称固定,比如hive-quality
	 * 获取数据源id和租户id
	 * ③post创建工作流
	 * ④get查询流程定义列表(获取工作流ID)
	 * ⑤post工作流定时
	 * ⑥查询定时任务列表(为了获取定时任务id) Copy
	 * ⑦post定时任务上线
	 */


	public static void creat_project(String http,String Accept,String token,String  projectName){
		System.out.println("--------------------------------post请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("projectName", projectName);//表单参数,body
		getParamMaps.put("description","数据质量检测");

		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/create")
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
	}




	public static void creat_workflow(String http,String Accept,String token,String workflow_name,String projectName,List<Integer> taskid_list,List<Integer> nodeid_list,List<String> SQL_list,String tenantCode,Integer datasourceId,String worker_group, ArrayList<String> rule_list){    //拼好的SQL作为入参
		System.out.println("--------------------------------post请求-----------------------------------");
		int x=100;
		int y=0;
		String loactionstr="";
		for (int i = 0; i < taskid_list.size(); i++,y+=100) {     //for loca
			// String loca = String.format("{\"taskCode\":%s,\"x\":%s,\"y\":%s},", taskid_list.get(i), x, y);
			if (i==0){    //一个前置SQL
				String loca = String.format("\"%s\":{\"name\":\"%s\",\"targetarr\":\"\",\"nodenumber\":\"0\",\"x\":%s,\"y\":%s},","tasks-"+taskid_list.get(i), rule_list.get(i),x-100, y);
				loactionstr=loactionstr+loca;
			}else{       //其余后置
				String loca = String.format("\"%s\":{\"name\":\"%s\",\"targetarr\":\"tasks-10000\",\"nodenumber\":\"0\",\"x\":%s,\"y\":%s},","tasks-"+taskid_list.get(i), rule_list.get(i),x, y);
				loactionstr=loactionstr+loca;
			}
		}
		loactionstr= HiveUtils.replaceLast(loactionstr, ",", "");
		loactionstr="{"+loactionstr+"}";
		System.out.println(loactionstr);  //测试

//           {"tasks-97765":{"name":"SQL1","targetarr":"","nodenumber":"0","x":213,"y":93},
//            "tasks-94966":{"name":"SQL2","targetarr":"tasks-97765","nodenumber":"0","x":382,"y":59},
//            "tasks-78825":{"name":"SQL3","targetarr":"tasks-97765","nodenumber":"0","x":376,"y":163}}

		String relastr="";
		int first_id = taskid_list.get(0);
		for (int i = 1; i < taskid_list.size(); i++) {     //for defin
			String rela = String.format("{\"endPointSourceId\":\"%s\",\"endPointTargetId\":\"%s\"},", "tasks-"+first_id,"tasks-"+taskid_list.get(i) );
			relastr=relastr+rela;
		}
		relastr= HiveUtils.replaceLast(relastr, ",", "");
		relastr="["+relastr+"]";
		System.out.println(relastr);  //测试

		//[{"endPointSourceId":"tasks-97765","endPointTargetId":"tasks-94966"},{"endPointSourceId":"tasks-97765","endPointTargetId":"tasks-78825"}]

		String definestr="";
		int name =0;
		for (int i = 0; i < taskid_list.size(); i++,name++) {     //for defin  name += 1
			//不返回数据的,SQL_type为1  ,select SQL_type为0
			// name=RandomUtil.getRandom().nextInt(10000, 99999);
			if (i==0){    //一个前置SQL
				String defin = String.format("{\"type\":\"SQL\",\"id\":\"%s\",\"name\":\"%s\",\"params\":{\"type\":\"%s\",\"datasource\":%s,\"sql\":\"%s\",\"udfs\":\"\",\"sqlType\":\"1\",\"title\":\"jieshouren\",\"receivers\":\"102030@qq.com\",\"receiversCc\":\"\",\"showType\":\"TABLE\",\"localParams\":[],\"connParams\":\"\",\"preStatements\":[],\"postStatements\":[]},\"description\":\"\",\"timeout\":{\"strategy\":\"\",\"interval\":null,\"enable\":false},\"runFlag\":\"NORMAL\",\"conditionResult\":{\"successNode\":[\"\"],\"failedNode\":[\"\"]},\"dependence\":{},\"maxRetryTimes\":\"0\",\"retryInterval\":\"1\",\"taskInstancePriority\":\"MEDIUM\",\"workerGroup\":\"%s\",\"preTasks\":[]},", "tasks-"+taskid_list.get(i), "SQL"+nodeid_list.get(i),"HIVE",datasourceId, SQL_list.get(i),worker_group);
				definestr = definestr + defin;
			}else{       //其余后置
				String defin = String.format("{\"type\":\"SQL\",\"id\":\"%s\",\"name\":\"%s\",\"params\":{\"type\":\"%s\",\"datasource\":%s,\"sql\":\"%s\",\"udfs\":\"\",\"sqlType\":\"1\",\"title\":\"jieshouren\",\"receivers\":\"102030@qq.com\",\"receiversCc\":\"\",\"showType\":\"TABLE\",\"localParams\":[],\"connParams\":\"\",\"preStatements\":[],\"postStatements\":[]},\"description\":\"\",\"timeout\":{\"strategy\":\"\",\"interval\":null,\"enable\":false},\"runFlag\":\"NORMAL\",\"conditionResult\":{\"successNode\":[\"\"],\"failedNode\":[\"\"]},\"dependence\":{},\"maxRetryTimes\":\"0\",\"retryInterval\":\"1\",\"taskInstancePriority\":\"MEDIUM\",\"workerGroup\":\"%s\",\"preTasks\":[\"%s\"]},", "tasks-"+taskid_list.get(i), "SQL"+nodeid_list.get(i),"HIVE",datasourceId, SQL_list.get(i),worker_group,"SQL"+nodeid_list.get(0));
				definestr = definestr + defin;
			}

		}
		definestr= HiveUtils.replaceLast(definestr, ",", "");

		definestr= String.format("{\"globalParams\":[], \"tasks\":[%s],\"tenantId\":%s,\"timeout\":0}",definestr,10);
		System.out.println(definestr);  //测试

//        {"globalParams":[],
//            "tasks":[
//                    {"type":"SQL","id":"tasks-97765","name":"SQL1","params":{"type":"HIVE","datasource":6,"sql":"select+*+from+AA","udfs":"","sqlType":"0","title":"hive1","receivers":"10203@qq.com","receiversCc":"","showType":"TABLE","localParams":[],"connParams":"","preStatements":[],"postStatements":[]},"description":"","timeout":{"strategy":"","interval":null,"enable":false},"runFlag":"NORMAL","conditionResult":{"successNode":[""],"failedNode":[""]},"dependence":{},"maxRetryTimes":"0","retryInterval":"1","taskInstancePriority":"MEDIUM","workerGroup":"default","preTasks":[]},
//                    {"type":"SQL","id":"tasks-94966","name":"SQL2","params":{"type":"HIVE","datasource":6,"sql":"select+*+from+AA","udfs":"","sqlType":"0","title":"123","receivers":"102030@qq.com","receiversCc":"","showType":"TABLE","localParams":[],"connParams":"","preStatements":[],"postStatements":[]},"description":"","timeout":{"strategy":"","interval":null,"enable":false},"runFlag":"NORMAL","conditionResult":{"successNode":[""],"failedNode":[""]},"dependence":{},"maxRetryTimes":"0","retryInterval":"1","taskInstancePriority":"MEDIUM","workerGroup":"default","preTasks":["SQL1"]},
//                    {"type":"SQL","id":"tasks-78825","name":"SQL3","params":{"type":"HIVE","datasource":6,"sql":"select+*+from+t_b_basic","udfs":"","sqlType":"0","title":"123","receivers":"102030@qq.com","receiversCc":"","showType":"TABLE","localParams":[],"connParams":"","preStatements":[],"postStatements":[]},"description":"","timeout":{"strategy":"","interval":null,"enable":false},"runFlag":"NORMAL","conditionResult":{"successNode":[""],"failedNode":[""]},"dependence":{},"maxRetryTimes":"0","retryInterval":"1","taskInstancePriority":"MEDIUM","workerGroup":"default","preTasks":["SQL1"]}
//            ],
//            }

		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("locations", loactionstr); //表单参数,body
		getParamMaps.put("name", workflow_name);     //OK
		getParamMaps.put("processDefinitionJson", definestr);
		getParamMaps.put("connects",relastr);
		getParamMaps.put("description", "质量处理");

		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/"+projectName+"/process/save")
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
	}

	public  static String get_workflowid(String http,String Accept,String token,String  projectName,String workflow_name){
		System.out.println("--------------------------------get请求-----------------------------------");
		//   http://10.0.7.59:12345/dolphinscheduler/projects/{projectName}/process/list

		HashMap<String, Object> getParamMaps = new HashMap<>();

		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/projects/"+projectName+"/process/list")
				.header("token", token)    //请求头
				.header("Accept", Accept)
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();
		System.out.println(jsonobj);

		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			Object data = body.get("data");
			JSONArray dataobj = new JSONArray(data);
			for (Object o : dataobj) {
				JSONObject entries = new JSONObject(o);
				if ( workflow_name.equals(entries.get("name").toString())){
					return  entries.get("id").toString();
				}
			}
		}
		return "0";
	}



	public  static void workflow_online(String http,String Accept,String token,String workflow_id,String  projectName,String operator){
		System.out.println("--------------------------------post请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("processId", workflow_id);//表单参数,body
		getParamMaps.put("releaseState", operator);

		//   http://10.0.7.59:12345/dolphinscheduler/projects/{projectName}/process/release

		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/"+projectName+"/process/release")
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
	}



	public static void time_set(String http,String Accept,String token,String workflow_id,String  projectName,String cron){
		System.out.println("--------------------------------post请求-----------------------------------");
//        JSONArray schedule = new JSONArray();
//
//        schedule.add(new JSONObject().
//                putOnce("schedule", new JSONObject().
//                        putOnce("startTime", "2022-06-30 00:00:00").
//                        putOnce("endTime", "2122-06-30 00:00:00").
//                        putOnce("crontab", "0 0 1 * * ? *").
//                        putOnce("timezoneId", "Asia/Shanghai"))
//        );
//   String crontab="0 0 1 * * ? *";
		String time = String.format("{\"startTime\":\"2022-07-26 00:00:00\",\"endTime\":\"2122-07-26 00:00:00\",\"crontab\":\"%s\"}", cron);
		//空格为什么变成了加号
		//{"startTime":"2022-07-26 00:00:00","endTime":"2122-07-26 00:00:00","crontab":"0 0 * * * ? *"}
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("schedule", time);//表单参数,body
		getParamMaps.put("failureStrategy", "CONTINUE");
		getParamMaps.put("warningType", "NONE");
		getParamMaps.put("processInstancePriority", "MEDIUM");
		getParamMaps.put("warningGroupId", "0");
		getParamMaps.put("receivers", "");
		getParamMaps.put("receiversCc", "");
		getParamMaps.put("workerGroup", "default");
		getParamMaps.put("processDefinitionId", workflow_id);


		//   http://10.0.7.59:12345/dolphinscheduler/projects/{projectName}/schedule/create

		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/"+projectName+"/schedule/create")
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);

	}


	public static String old_getTimesetId(String http,String Accept,String token,String projectName){
		System.out.println("--------------------------------post请求-----------------------------------");
		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/"+projectName+"/schedule/list")
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.execute();

		//  http://10.0.7.59:12345/dolphinscheduler/projects/{projectName}/schedule/list

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			Object data = body.get("data");
			JSONArray dataobj = new JSONArray(data);
			JSONObject entries = new JSONObject(dataobj.get(0));
			String timeset_id = entries.get("id").toString();
			return timeset_id;
		}
		return "0";
	}


	public static String getTimesetId(String http,String Accept,String token,String projectName,String workflow_id){
		System.out.println("--------------------------------get请求-----------------------------------");

		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("processDefinitionId", workflow_id);//表单参数,body
		getParamMaps.put("pageNo", "1");
		getParamMaps.put("pageSize", "10");


		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/projects/"+projectName+"/schedule/list-paging")
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.form(getParamMaps)
				.execute();

		//  http://10.0.7.59:12345/dolphinscheduler/projects/{projectName}/schedule/list

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			JSONObject data = new JSONObject(body.get("data"));
			JSONArray dataobj = new JSONArray(data.get("totalList"));
			JSONObject entries = new JSONObject(dataobj.get(0));
			String timeset_id = entries.get("id").toString();
			return timeset_id;
		}
		return "0";
	}



	public static void timeset_online(String http,String Accept,String token,String projectName,String timeset_id){
		System.out.println("--------------------------------post请求-----------------------------------");

		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("id", timeset_id);//表单参数,body

		HttpResponse getResponse = HttpRequest.post(http+"/dolphinscheduler/projects/"+projectName+"/schedule/online")
				//http://10.0.7.59:12345/dolphinscheduler/projects/{projectName}/schedule/online
				.header("Accept", Accept)
				.header("token", token)    //请求头
				.form(getParamMaps)
				.execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
	}

	public static String get_log(String http,String token,String projectName,String taskInstanceId,Long skipLineNum,Long limit){
		System.out.println("--------------------------------get请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("taskInstanceId",taskInstanceId);//实例ID
		getParamMaps.put("skipLineNum",skipLineNum);
		getParamMaps.put("limit",limit);

		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/log/detail")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		System.out.println("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		System.out.println(jsonobj);
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			Object data = body.get("data");
			//System.out.println(data.toString());
			//JSONObject dataobj = new JSONObject(data);
			return data.toString();
		}

		return "0";
	}
	//1、任务名称searchVal-任务名称    2、状态stateType-state 目前两种执行中（1） 已结束（2）
	public static String get_status(String http,String token,String projectName,String workflowId ,
									String startDate , String endDate , String stateType, String taskName,
									Integer pageSize ,Integer pageNO) {
		log.info("--------------------dolphinscheduler--get请求-----------------------------------");
		HashMap<String, Object> getParamMaps = new HashMap<>();
		getParamMaps.put("processDefinitionId",workflowId);
		getParamMaps.put("startDate",startDate);
		getParamMaps.put("endDate",endDate);
		if (stateType != null && !stateType.equals("")) {
			switch (Integer.parseInt(stateType)) {
				case 1:
					getParamMaps.put("stateType", "RUNNING_EXEUTION");
					break;
				case 2:
					getParamMaps.put("stateType", "FAILURE");
					break;
			}
		} else {
			getParamMaps.put("stateType", null);
		}
		getParamMaps.put("pageSize",pageSize != null ? pageSize : 10);
		getParamMaps.put("pageNo",pageNO != null ? pageNO :1);
		getParamMaps.put("searchVal", taskName);
		HttpResponse getResponse = HttpRequest.get(http+"/dolphinscheduler/projects/"+projectName+"/instance/list-paging")
				.header("Accept", "application/json")
				.header("token", token)    //请求头
				.form(getParamMaps).execute();

		int status = getResponse.getStatus();   //状态码
		log.info("请求响应状态码:" + status);
		String jsonobj = getResponse.body();    //相应体中包含project-id
		JSONObject body = new JSONObject(jsonobj);
		Object msg = body.get("msg");
		if("success".equals(msg)){
			Object data = body.get("data");
			String s = com.alibaba.fastjson.JSONObject.toJSONString(data);
			return s;
		}
		return "0";
	}
}

