package com.test.core.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.test.core.util.ObjectUtils;

@Component
public class HbaseService {
	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseService.class);

	/**
	 * 建表时要指定的是：表名、列族(可多个，一般建议不要设置多个列族。) 建表语句 create 'user_info', 'base_info',
	 * 'ext_info' 意思是新建一个表，名称是user_info，包含两个列族base_info和ext_info 列族
	 * 是列的集合，一个列族中包含多个列 这时的表结构： row key,base_info,ext_info row key
	 * 是行键，每一行的ID，这个字段是自动创建的，建表时不需要指定
	 */
	@Autowired
	private org.apache.hadoop.hbase.client.Connection hbaseConnection;

	private static org.apache.hadoop.hbase.client.Connection connection;

	public static Admin admin = null;

	/**
	 * @PostContruct是spring框架的注解 spring容器初始化的时候执行该方法
	 */
	@PostConstruct
	public void init() {
		try {
			connection = this.hbaseConnection;
			if (connection != null) {
				admin = connection.getAdmin();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*******************************
	 * admin Start
	 **************************************/

	/**
	 * 建表
	 * 
	 * @param table
	 *            表名
	 * @param familyName
	 *            列族名
	 * @return
	 */
	public static boolean createTable(String table, String familyName) {
		boolean flag = false;
		if (admin != null) {
			try {
				TableName tableName = TableName.valueOf(table);
				if (admin.tableExists(tableName)) {
					flag = true;
					System.out.println("talbe is exists!");
				} else {
					// creating table descriptor
					TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor = new TableDescriptorBuilder.ModifyableTableDescriptor(
							tableName);

					// creating column family descriptor
					ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor family = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
							Bytes.toBytes(familyName));
					// adding coloumn family to HTable
					tableDescriptor.setColumnFamily(family);
					// 创建表
					admin.createTable(tableDescriptor);
					// 表存在则创建成功
					flag = admin.tableExists(tableName);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		return flag;
	}

	/**
	 * 建表
	 * 
	 * @param table
	 *            表名
	 * @param familys
	 *            列族名（多个，也可以是数组）
	 * @return
	 */
	public static boolean createTable(String table, String... familys) {
		boolean flag = false;
		if (admin != null) {
			try {
				TableName tableName = TableName.valueOf(table);

				if (admin.tableExists(tableName)) {
					System.out.println("talbe is exists!");
				} else {

					HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
					for (String family : familys) {
						HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
						hTableDescriptor.addFamily(hColumnDescriptor);
					}
					admin.createTable(hTableDescriptor);
					// 表存在则创建成功
					flag = admin.tableExists(tableName);
				}
			} catch (Exception e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 查看已有表
	 * 
	 * @return
	 */
	public static TableName[] getAllTables() {
		if (admin != null) {
			try {
				// 获取所有表名
				TableName[] tableNames = admin.listTableNames();
				// TableName tb = tableNames[0];
				return tableNames;
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 查看已有表
	 * 
	 * @return
	 */
	public static HTableDescriptor[] listAllTables() {
		if (admin != null) {
			try {
				HTableDescriptor hTableDescriptors[] = admin.listTables();
				/**
				 * for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
				 * System.out.println(hTableDescriptor.getNameAsString()); }
				 **/
				return hTableDescriptors;
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 禁用表
	 * 
	 * @param table
	 * @return
	 */
	public static boolean disableTable(String table) {
		boolean flag = false;
		if (admin != null) {
			try {
				TableName tableName = TableName.valueOf(table);
				// 判断table是否启用
				if (admin.isTableEnabled(tableName)) {
					// 禁用表table
					admin.disableTable(tableName);
					// 判断table是否启用
					flag = !admin.isTableEnabled(tableName);
				} else {
					flag = true;
				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 启用表
	 * 
	 * @param table
	 * @return
	 */
	public static boolean enableTable(String table) {
		boolean flag = false;
		if (admin != null) {
			try {
				TableName tableName = TableName.valueOf(table);
				// 判断table是否启用

				if (!admin.isTableDisabled(tableName)) {
					// 启用表
					admin.enableTable(tableName);
					// 判断table是否启用
					flag = admin.isTableEnabled(tableName);
				} else {
					flag = true;
				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 添加列族
	 * 
	 * @param table
	 * @param columnFamily
	 */
	public static void addColumnFamily(String table, String columnFamily) {
		if (table != null && !"".equals(table) && columnFamily != null && !"".equals(columnFamily)) {
			try {
				TableName tableName = TableName.valueOf(table);
				// 列信息
				ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor coums = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
						Bytes.toBytes(columnFamily));

				// 指定某列最大版本号
				coums.setMaxVersions(9);
				// 指定某列当前版本及最大版本
				coums.setVersions(1, 9);
				// 添加列族
				admin.addColumnFamily(tableName, coums);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 删除表
	 * 
	 * @param tableName
	 */
	public static void deleteTable(String tableName) {
		if (admin != null) {
			try {
				TableName table = TableName.valueOf(tableName);
				if (admin.tableExists(table)) {
					admin.disableTable(table);
					admin.deleteTable(table);
				}
			} catch (IOException e) {
				LOGGER.debug("<error> deleteTable catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
	}

	/*******************************
	 * admin End
	 **************************************/

	/*******************************
	 * connection Start
	 **************************************/
	// 格式化输出
	public static void showCell(Result result) {
		// 获取结果集
		// byte[] value = result.getValue(Bytes.toBytes("personal"),
		// Bytes.toBytes("name"));
		// byte[] value1 = result.getValue(Bytes.toBytes("personal"),
		// Bytes.toBytes("city"));
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timetamp:" + cell.getTimestamp() + " ");
			System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}

	public static  ArrayList<Map<String, Map<String, Object>>> getResultMap(ResultScanner resultScanner) {
		 ArrayList<Map<String, Map<String, Object>>> mapArrList =null;
		if (resultScanner != null) {
			// 迭代结果
			 mapArrList = new ArrayList<>();
			for (Result result : resultScanner) {
				Cell[] cells = result.rawCells();
				
				if (cells != null && cells.length > 0) {
					
					Map<String, Map<String, Object>> mapFamily = new HashMap<String, Map<String, Object>>();
					Map<String, Object> map = null;
					for (Cell cell : cells) {

						// System.out.println("RowKey:" + new
						// String(CellUtil.cloneRow(cell)) + " ");
						// System.out.println("Timetamp:" + cell.getTimestamp()
						// + " ");
						// System.out.println("column Family:" + newString(CellUtil.cloneFamily(cell)) + " ");
						// System.out.println("row Name:" + new
						// String(CellUtil.cloneQualifier(cell)) + " ");
						// System.out.println("value:" + new
						// String(CellUtil.cloneValue(cell)) + " ");
//System.out.println(new String(CellUtil.cloneRow(cell))+" | "+new String(CellUtil.cloneFamily(cell))+" | "+new String(CellUtil.cloneQualifier(cell)));
						 
						if(mapFamily.get(new String(CellUtil.cloneFamily(cell)))!=null){
							map = mapFamily.get(new String(CellUtil.cloneFamily(cell)));
						}else{
							map = new HashMap<String, Object>();
						}
							
						
						// name value
						map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
						mapFamily.put(new String(CellUtil.cloneFamily(cell)), map);
					}

					//mapFamily.put("rowKey", new String(CellUtil.cloneRow(cell)));
					//mapFamily.put("timetamp", cell.getTimestamp());
					mapArrList.add(mapFamily);
				}
			}
		}

		return mapArrList;
	}

	public static Map<String, Map<String, Object>> getResultMap(Result result) {

		// 获取结果集
		// byte[] value = result.getValue(Bytes.toBytes("personal"),
		// Bytes.toBytes("name"));
		// byte[] value1 = result.getValue(Bytes.toBytes("personal"),
		// Bytes.toBytes("city"));
		Map<String, Map<String, Object>> familyMap = null;
		Cell[] cells = result.rawCells();
		if (cells != null && cells.length > 0) {
			familyMap = new HashMap<String, Map<String, Object>>();

			for (Cell cell : cells) {

				// System.out.println("RowKey:" + new
				// String(CellUtil.cloneRow(cell)) + " ");
				// System.out.println("Timetamp:" + cell.getTimestamp() + " ");
				// System.out.println("column Family:" + new
				// String(CellUtil.cloneFamily(cell)) + " ");
				// System.out.println("row Name:" + new
				// String(CellUtil.cloneQualifier(cell)) + " ");
				// System.out.println("value:" + new
				// String(CellUtil.cloneValue(cell)) + " ");

				Map<String, Object> map = null;
				if (familyMap != null && familyMap.get(new String(CellUtil.cloneFamily(cell))) != null) {
					map = familyMap.get(new String(CellUtil.cloneFamily(cell)));
					map.put("rowKey", new String(CellUtil.cloneRow(cell)));
					map.put("timetamp", cell.getTimestamp());
					// name value
					map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
				} else {
					map = new HashMap<String, Object>();
					map.put("rowKey", new String(CellUtil.cloneRow(cell)));
					map.put("timetamp", cell.getTimestamp());
					// name value
					map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
				}

				if (map != null) {
					familyMap.put(new String(CellUtil.cloneFamily(cell)), map);
				}

			}
		}
		return familyMap;
	}

	/**
	 * 根据rowkey查询数据
	 * 
	 * @param tableName
	 * @param rowkey
	 * @return
	 */
	public static Map<String, Map<String, Object>> getListByRowkey(String tableName, String rowkey) {
		Map<String, Map<String, Object>> familyMap = null;
		try {
			if (connection != null) {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					Get get = new Get(Bytes.toBytes(rowkey));
					// 获取指定列族数据
					// get.addFamily(Bytes.toBytes(colFamily));
					// 获取指定列数据
					// get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
					Result result = table.get(get);
					// showCell(result);
					familyMap = getResultMap(result);
					table.close();
				}
			}
		} catch (IOException e) {
			LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
			e.printStackTrace();
		}
		return familyMap;

	}

	/**
	 * 根据rowkey查询指定列族数据
	 * 
	 * @param tableName
	 * @param rowkey
	 * @return
	 */
	public static Map<String, Map<String, Object>> getListByRowkey(String tableName, String colFamily, String col,
			String rowkey) {
		Map<String, Map<String, Object>> familyMap = null;
		try {
			if (connection != null) {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					Get get = new Get(Bytes.toBytes(rowkey));
					// 获取指定列族下所有列数据
					get.addFamily(Bytes.toBytes(colFamily));
					// 获取指定列族下指定列
					get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
					Result result = table.get(get);
					// showCell(result);
					familyMap = getResultMap(result);
					table.close();
				}
			}
		} catch (IOException e) {
			LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
			e.printStackTrace();
		}
		return familyMap;

	}

	/**
	 * 
	 * 插入数据
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param col
	 * @param val
	 */
	public static void insterRow(String tableName, String rowkey, String colFamily, String col, String val) {
		if (connection != null) {
			try {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					Put put = new Put(Bytes.toBytes(rowkey));
					put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
					table.put(put);
				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}

		// 批量插入
		/*
		 * List<Put> putList = new ArrayList<Put>(); puts.add(put);
		 * table.put(putList);
		 */
	}

	/**
	 * 批量插入javaBean
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param object
	 */
	public static void insterObject(String tableName, String rowkey, String colFamily, Object object) {
		if (connection != null) {
			try {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					// 批量插入
					List<Put> putList = new ArrayList<Put>();
					Map<String, String> colMap = ObjectUtils.objectToMapString(object);
					if (colMap != null) {
						Put put = new Put(Bytes.toBytes(rowkey));
						for (Map.Entry<String, String> entry : colMap.entrySet()) {
							if(entry.getKey()!=null && entry.getValue()!=null && !"".equals(entry.getValue()) && !"null".equals(entry.getValue())){
								
								put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey()),
										Bytes.toBytes(entry.getValue()));
								// System.out.println("Key = " + entry.getKey() + ",
								// Value = " + entry.getValue());
							
							}
						}
						putList.add(put);
					}
					table.put(putList);
				}
			} catch (Exception e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}

	}
	
	
	
	public static void insterObject(String tableName,  String colFamily, Object object) {
		String rowkey= UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
		insterObject(tableName, rowkey, colFamily, object);
	}

	/**
	 * 批量插入
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param colMap
	 */
	public static void insterBatchRowMap(String tableName, String rowkey, String colFamily,
			Map<String, String> colMap) {
		if (connection != null) {
			try {
				if (colMap != null) {
					Table table = connection.getTable(TableName.valueOf(tableName));
					if (table != null) {
						// 批量插入
						List<Put> putList = new ArrayList<Put>();

						for (Map.Entry<String, String> entry : colMap.entrySet()) {
							Put put = new Put(Bytes.toBytes(rowkey));
							put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey()),
									Bytes.toBytes(entry.getValue()));
							// System.out.println("Key = " + entry.getKey() + ",
							// Value = " + entry.getValue());
							putList.add(put);
						}
						table.put(putList);
					}
				}

			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}

	}

	/**
	 * 批量插入
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param colMapList
	 */
	public static void insterBatchRowMapList(String tableName, String colFamily, List<Map<String, String>> colMapList) {
		if (connection != null) {
			try {
				if (colMapList != null && colMapList.size() > 0) {
					Table table = connection.getTable(TableName.valueOf(tableName));
					if (table != null) {
						// 批量插入
						List<Put> putList = new ArrayList<Put>();
						for (int i = 0; i < colMapList.size(); i++) {
							Map<String, String> colMap = colMapList.get(i);
							if (colMap != null) {

								String rowkey = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();

								Put put = new Put(Bytes.toBytes(rowkey));
								for (Map.Entry<String, String> entry : colMap.entrySet()) {

									put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey()),
											Bytes.toBytes(entry.getValue()));
									// System.out.println("Key = " +
									// entry.getKey()
									// + ", Value = " + entry.getValue());
								}
								putList.add(put);
							}
						}
						table.put(putList);
					}
				}

			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}

	}

	/**
	 * 批量插入
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param colObjectList
	 */
	public static void insterBatchObjectList(String tableName, String colFamily, List<?> colObjectList) {
		if (connection != null) {
			try {
				if (colObjectList != null && colObjectList.size() > 0) {
					Table table = connection.getTable(TableName.valueOf(tableName));
					if (table != null) {
						// 批量插入
						List<Put> putList = new ArrayList<Put>();
						for (int i = 0; i < colObjectList.size(); i++) {
							Object object = colObjectList.get(i);
							Map<String, String> colMap = ObjectUtils.objectToMapString(object);
							if (colMap != null) {
								String rowkey = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
								Put put = new Put(Bytes.toBytes(rowkey));
								for (Map.Entry<String, String> entry : colMap.entrySet()) {
									if(entry.getKey()!=null && entry.getValue()!=null && !"".equals(entry.getValue()) && !"null".equals(entry.getValue())){
										put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey()),
												Bytes.toBytes(entry.getValue()));
										// System.out.println("Key = " +
										// entry.getKey()
										// + ", Value = " + entry.getValue());
									}
									putList.add(put);
								}
							}
						}
						table.put(putList);
					}
				}

			} catch (Exception e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}

	}

	/**
	 * 删除
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param col
	 */
	public static void deleteRow(String tableName, String rowkey, String colFamily, String col) {
		if (connection != null) {
			try {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					Delete delete = new Delete(Bytes.toBytes(rowkey));
					if (colFamily != null && !"".equals(colFamily)) {
						// 删除指定列族
						delete.addFamily(Bytes.toBytes(colFamily));

						if (col != null && !"".equals(col)) {
							// 删除指定列
							delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
						}

					}

					table.delete(delete);
					// 批量删除
					/*
					 * List<Delete> deleteList = new ArrayList<Delete>();
					 * deleteList.add(delete); table.delete(deleteList);
					 */
				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
	}

	/**
	 * 删除指定的rowkey行
	 * 
	 * @param tableName
	 * @param rowkey
	 */
	public static void deleteRow(String tableName, String rowkey) {
		deleteRow(tableName, rowkey, null, null);
	}

	/**
	 * 删除指定行族的rowkey行
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 */
	public static void deleteRow(String tableName, String rowkey, String colFamily) {
		deleteRow(tableName, rowkey, colFamily, null);
	}

	/**
	 * 根据rowkey查找数据
	 * 
	 * @param tableName
	 * @param rowkey
	 * @return
	 */
	public static Map<String, Map<String, Object>> getData(String tableName, String rowkey) {
		return getData(tableName, rowkey, null, null, 0);
	}

	/**
	 * 查找指定列数据
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param col
	 * @return
	 */
	public static Map<String, Map<String, Object>> getData(String tableName, String rowkey, String colFamily,
			String col) {
		return getData(tableName, rowkey, colFamily, col, 0);
	}

	public static Map<String, Map<String, Object>> getData(String tableName) {
		return getData(tableName, null, null, null, 0);
	}

	/**
	 * 查找指定列数据
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param colFamily
	 * @param col
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Map<String, Map<String, Object>> getData(String tableName, String rowkey, String colFamily,
			String col, int versions) {
		Map<String, Map<String, Object>> familyMap = null;
		if (connection != null) {
			try {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					if (table != null) {
						Get get = null;
						if (rowkey != null && !"".equals(rowkey)) {
							get = new Get(Bytes.toBytes(rowkey));
						}

						if (colFamily != null && !"".equals(colFamily)) {
							// 获取指定列族数据
							get.addFamily(Bytes.toBytes(colFamily));
							if (col != null && !"".equals(col)) {
								// 获取指定列数据
								get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
							}
						}

						if (versions > 0) {
							// 查询某列数据的某个版本
							get.setMaxVersions(versions);
						}
						Result result = table.get(get);
						// showCell(result);
						familyMap = getResultMap(result);
						table.close();
					}
				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return familyMap;
	}

	/**
	 * 检索指定行族的列数据
	 * 
	 * @param tableName
	 * @param colFamily
	 * @param col
	 * @return
	 */
	public static ArrayList<Map<String, Map<String, Object>>> scanData(String tableName, String colFamily, String col,
			int versions) {
		ArrayList<Map<String, Map<String, Object>>> familyMap = null;
		if (connection != null) {
			try {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					// 扫描
					Scan scan = new Scan();
					// 扫描指定列簇
					if (colFamily != null && !"".equals(colFamily)) {
						scan.addFamily(Bytes.toBytes(colFamily));
						if (col != null && !"".equals(col)) {
							// 扫描指定列
							scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
						}
					}

					if (versions > 0) {
						// 查询某列数据的某个版本
						scan.setMaxVersions(versions);
					}

					// 扫描结果集
					ResultScanner resultScanner = table.getScanner(scan);

					familyMap = getResultMap(resultScanner);

				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
		return familyMap;
	}

	/**
	 * 检索指定行族数据
	 * 
	 * @param tableName
	 * @param colFamily
	 * @return
	 */
	public static ArrayList<Map<String, Map<String, Object>>> scanData(String tableName, String colFamily) {
		return scanData(tableName, colFamily, null, 0);
	}
	/**
	 * 检索数据
	 * 
	 * @param tableName
	 * @param colFamily
	 * @return
	 */
	public static ArrayList<Map<String, Map<String, Object>>> scanData(String tableName) {
		return scanData(tableName, null, null, 0);
	}

	// 批量查找数据
	public static void scanDataBetwenRow(String tableName, String startRow, String stopRow) {
		if (connection != null) {
			try {
				Table table = connection.getTable(TableName.valueOf(tableName));
				if (table != null) {
					Scan scan = new Scan();
					scan.setStartRow(Bytes.toBytes(startRow));
					scan.setStopRow(Bytes.toBytes(stopRow));
					ResultScanner resultScanner = table.getScanner(scan);
					for (Result result : resultScanner) {
						showCell(result);
					}
				}
			} catch (IOException e) {
				LOGGER.debug("<error>  catch message : " + e.toString() + "</error>");
				e.printStackTrace();
			}
		}
	}

}
