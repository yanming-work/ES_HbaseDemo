package com.test.core.result;
import java.util.LinkedHashMap;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.test.core.util.FastJsonUtils;



public class JqGridUtil {

	private static String DBType= "oracle";
	/**
	 * 
	* @Title: 获取 tJqGrid参数，生成SQL语句 
	* @Description: TODO(这里用一句话描述这个方法的作用) 
	* @param @param request
	* @param @param tableName
	* @param @return   0 完整SQL   1 where语句
	* @return String[]    返回类型 
	* @throws
	 */
	@SuppressWarnings("unused")
	public static String[] getJqGridParameterToSql(JSONObject parameterJSONObj ,String tableName,@SuppressWarnings("rawtypes") Class clzz)
	{
		String page = "1";
		String rows ="20";
		String sidx =null;
		String sord = null;
		String _search = null;		
		String searchField =null;
		String searchString = null;
		String searchOper = null;
		String filters = null;
		
		
		
		if(parameterJSONObj!=null ){
			 page = parameterJSONObj.getString("page")  == null?"1":parameterJSONObj.getString("page");	
			 rows = parameterJSONObj.getString("rows")  == null?"20":parameterJSONObj.getString("rows");
			 
			 sidx = parameterJSONObj.getString("sidx") ;
			sord = parameterJSONObj.getString("sord") ;
			 _search = parameterJSONObj.getString("_search") ;
				//单字段查询
				 searchField = parameterJSONObj.getString("searchField") ;
				 searchString = parameterJSONObj.getString("searchString") ;
				 searchOper = parameterJSONObj.getString("searchOper") ;
				
				
				//多字段查询
				
				 filters = parameterJSONObj.getString("filters") ;
				
			
		}
			 //String className = clzz.getSimpleName();  
			String[]  sql=new String[3];
			
				if("oracle".equals(DBType)){
					
					int rn=(Integer.valueOf(page)-1)*Integer.valueOf(rows);
					int rownum=Integer.valueOf(page)*Integer.valueOf(rows);
					String orderbystr="";
					if(sidx!=null&&!"".equals(sidx)&&sord!=null&&!"".equals(sord)){
						orderbystr=" order by  "+sidx+" "+sord;
					}
					
					StringBuffer whereStr=new StringBuffer(" 1=1 ");
					if("true".equals(_search)){
						//单字段筛选
						if(searchField!=null&&searchString!=null){
							if(searchOper!=null){
								
								String searchWhereStr=getSearchWhereStr("and", searchOper, searchField, searchString,clzz);
								if(searchWhereStr!=null&&!"".equals(searchWhereStr)){
									whereStr.append(searchWhereStr);
								}
									
								
							}else{
								whereStr.append(" and "+searchField+"='"+searchString+"' ");
							}
						}
						//多字段筛选
						if(filters!=null){
							
							
							ParameterJqGridFilters parameterJqGridFilters=(ParameterJqGridFilters) FastJsonUtils.jsonStrToObject(filters, ParameterJqGridFilters.class);
							if(parameterJqGridFilters!=null){
								
								if(parameterJqGridFilters.getGroupOp()!=null&&parameterJqGridFilters.getRules()!=null&&parameterJqGridFilters.getRules().size()>0){
									List<ParameterJqGridSearchRules> rulesList=parameterJqGridFilters.getRules();
									whereStr.append(" and  ( 1=1  ");
									for (ParameterJqGridSearchRules parameterJqGridSearchRules : rulesList) {
										String op=parameterJqGridSearchRules.getOp();
										if(op!=null){
											
											String searchWhereStr=getSearchWhereStr(parameterJqGridFilters.getGroupOp(), parameterJqGridSearchRules.getOp(), parameterJqGridSearchRules.getField(), parameterJqGridSearchRules.getData(),clzz);
											if(searchWhereStr!=null&&!"".equals(searchWhereStr)){
												whereStr.append(searchWhereStr);
											}
											
										}
										
									}
									whereStr.append(" ) ");
								}
							}
						}
					}
					
					sql[0]=page;
					sql[1]=rows;
					sql[2]=whereStr.toString()+" "+orderbystr+"   ";
					
				}else if("mysql".equals(DBType)){
					//mysql生成sql
				}
			
			
		/**
			 * filters = 
		    {"groupOp":"AND",
		     "rules":[
		       {"field":"invdate","op":"ge","data":"2007-10-06"},
		       {"field":"invdate","op":"le","data":"2007-10-20"}, 
		       {"field":"name","op":"bw","data":"Client 3"}
		      ]
		    }
			 */
			
			return sql;
				
		
		
		
	}
	
	
	
	private static String getSearchWhereStr(String andor,String searchOper, String searchField,String searchString,@SuppressWarnings("rawtypes") Class clzz){
		
		 LinkedHashMap<String,LinkedHashMap<String,Object>>  fieldMap=ClassFieldHelper.initAnnoFieldDic(clzz);
		 //LinkedHashMap<String,Object> descriptionMap =null;
		 LinkedHashMap<String,Object> dbColumnNameMap =null;
		 if(fieldMap!=null){
			 // descriptionMap = fieldMap.get("description");
			  dbColumnNameMap = fieldMap.get("dbColumnNameMap");
			
		 }

		 String className = clzz.getSimpleName();  
		String wheresql=null;
		if(andor!=null&&!"".equals(andor) &&searchField!=null&&!"".equals(searchField)){
			if(dbColumnNameMap!=null){
				String searchFieldDB=dbColumnNameMap.get(className+"."+searchField)+"";
				if(searchFieldDB!=null&&!"".equals(searchFieldDB)){
					searchField=searchFieldDB;
				}
			}
			
			if(searchOper.trim().equals("eq")){ //等于            result = sField + "='" + sValue +"' "; 
				wheresql=" and "+searchField+" = '"+searchString+"' ";
			}  else if(searchOper.trim().equals("ne")){  //不等于           result = sField + " != '"+ sValue+"' "; 
					wheresql=" and "+searchField+" != '"+searchString+"' ";
			} else if(searchOper.trim().equals("lt")) { //小于            result = sField + " < '"+ sValue+"' "; 
					wheresql=" and "+searchField+" < '"+searchString+"' ";
			} else if(searchOper.trim().equals("le")){  //小于等于          result = sField + " <= '"+ sValue+"' ";
					wheresql=" and "+searchField+" <= '"+searchString+"' ";
			} else if(searchOper.trim().equals("gt")) { //大于            result = sField + " > '"+ sValue+"' "; 
					wheresql=" and "+searchField+" > '"+searchString+"' ";
			}else if(searchOper.trim().equals("ge")){  //大于等于          result = sField + " >= '"+ sValue+"' "; 
					wheresql=" and "+searchField+" >= '"+searchString+"' ";
			}else if(searchOper.trim().equals("bw")){  //以...开始            result = sField + " LIKE '"+ sValue+"%' "; 
					wheresql=" and "+searchField+" LIKE '"+searchString+"%' ";
			}  else if(searchOper.trim().equals("bn")){  //不以...开始           result = sField + " NOT LIKE '"+ sValue+"%' "; 
					wheresql=" and "+searchField+" NOT LIKE '"+searchString+"%' ";
			}else if(searchOper.trim().equals("in")){ //包括      
					wheresql=" and "+searchField+"  LIKE '%"+searchString+"%' ";
			}
		}
		
		return wheresql;
	}
}
