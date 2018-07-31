package com.test.core.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.*;

public class ObjectUtils {
	private static final String JAVAP = "java.";
	private static final String JAVADATESTR = "java.util.Date";

	/**
	 * 获取利用反射获取类里面的值和名称
	 *
	 * @param obj
	 * @return
	 * @throws IllegalAccessException
	 */
	public static Map<String, Object> objectToMapObject(Object obj) throws IllegalAccessException {
		Map<String, Object> map = null;
		if (obj != null) {
			map = new HashMap<String, Object>();
			Class<?> clazz = obj.getClass();
			System.out.println(clazz);
			for (Field field : clazz.getDeclaredFields()) {
				field.setAccessible(true);
				String fieldName = field.getName();
				Object value = field.get(obj);
				map.put(fieldName, value);
			}
		}
		return map;
	}

	public static Map<String, String> objectToMapString(Object obj) throws IllegalAccessException {
		Map<String, String> map = null;
		if (obj != null) {
			map = new HashMap<String, String>();
			Class<?> clazz = obj.getClass();
			System.out.println(clazz);
			for (Field field : clazz.getDeclaredFields()) {
				field.setAccessible(true);
				String fieldName = field.getName();
				String value = String.valueOf(field.get(obj));
				map.put(fieldName, value);
			}
		}

		return map;
	}

	/**
	 * 利用递归调用将Object中的值全部进行获取
	 *
	 * @param timeFormatStr
	 *            格式化时间字符串默认<strong>2017-03-10 10:21</strong>
	 * @param obj
	 *            对象
	 * @param excludeFields
	 *            排除的属性
	 * @return
	 * @throws IllegalAccessException
	 */
	public static Map<String, String> objectToMapString(String timeFormatStr, Object obj, String... excludeFields)
			throws IllegalAccessException {
		Map<String, String> map = new HashMap<>();

		if (excludeFields.length != 0) {
			List<String> list = Arrays.asList(excludeFields);
			objectTransfer(timeFormatStr, obj, map, list);
		} else {
			objectTransfer(timeFormatStr, obj, map, null);
		}
		return map;
	}

	/**
	 * 递归调用函数
	 *
	 * @param obj
	 *            对象
	 * @param map
	 *            map
	 * @param excludeFields
	 *            对应参数
	 * @return
	 * @throws IllegalAccessException
	 */
	private static Map<String, String> objectTransfer(String timeFormatStr, Object obj, Map<String, String> map,
			List<String> excludeFields) throws IllegalAccessException {
		boolean isExclude = false;
		// 默认字符串
		String formatStr = "YYYY-MM-dd HH:mm:ss";
		// 设置格式化字符串
		if (timeFormatStr != null && !timeFormatStr.isEmpty()) {
			formatStr = timeFormatStr;
		}
		if (excludeFields != null) {
			isExclude = true;
		}
		Class<?> clazz = obj.getClass();
		// 获取值
		for (Field field : clazz.getDeclaredFields()) {
			String fieldName = clazz.getSimpleName() + "." + field.getName();
			// 判断是不是需要跳过某个属性
			if (isExclude && excludeFields.contains(fieldName)) {
				continue;
			}
			// 设置属性可以被访问
			field.setAccessible(true);
			Object value = field.get(obj);
			Class<?> valueClass = value.getClass();
			if (valueClass.isPrimitive()) {
				map.put(fieldName, value.toString());

			} else if (valueClass.getName().contains(JAVAP)) {// 判断是不是基本类型
				if (valueClass.getName().equals(JAVADATESTR)) {
					// 格式化Date类型
					SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
					Date date = (Date) value;
					String dataStr = sdf.format(date);
					map.put(fieldName, dataStr);
				} else {
					map.put(fieldName, value.toString());
				}
			} else {
				objectTransfer(timeFormatStr, value, map, excludeFields);
			}
		}
		return map;
	}

	/**
	 * 使用org.apache.commons.beanutils进行转换
	 */
	public static Object mapToObject2(Map<String, Object> map, Class<?> beanClass) throws Exception {
		if (map == null)
			return null;

		Object obj = beanClass.newInstance();

		org.apache.commons.beanutils.BeanUtils.populate(obj, map);

		return obj;
	}

	/**
	 * 使用org.apache.commons.beanutils进行转换
	 */
	public static Map<?, ?> objectToMap2(Object obj) {
		if (obj == null)
			return null;

		return new org.apache.commons.beanutils.BeanMap(obj);
	}

	/**
	 * 使用Introspector进行转换
	 */
	public static Object mapToObject(Map<String, Object> map, Class<?> beanClass) throws Exception {
		if (map == null)
			return null;

		Object obj = beanClass.newInstance();

		BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		for (PropertyDescriptor property : propertyDescriptors) {
			Method setter = property.getWriteMethod();
			if (setter != null) {
				setter.invoke(obj, map.get(property.getName()));
			}
		}

		return obj;
	}

	/**
	 * 使用Introspector进行转换
	 */
	public static Map<String, Object> objectToMap(Object obj) throws Exception {
		if (obj == null)
			return null;

		Map<String, Object> map = new HashMap<String, Object>();

		BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		for (PropertyDescriptor property : propertyDescriptors) {
			String key = property.getName();
			// 默认PropertyDescriptor会有一个class对象，剔除之

			if (key.compareToIgnoreCase("class") == 0) {
				continue;
			}
			Method getter = property.getReadMethod();
			Object value = getter != null ? getter.invoke(obj) : null;
			map.put(key, value);
		}

		return map;
	}

	/**
	 * 使用reflect进行转换
	 */
	public static Object mapToObject3(Map<String, Object> map, Class<?> beanClass) throws Exception {
		if (map == null)
			return null;

		Object obj = beanClass.newInstance();

		Field[] fields = obj.getClass().getDeclaredFields();
		for (Field field : fields) {
			int mod = field.getModifiers();
			if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
				continue;
			}

			field.setAccessible(true);
			field.set(obj, map.get(field.getName()));
		}

		return obj;
	}

	/**
	 * 使用reflect进行转换
	 */
	public static Map<String, Object> objectToMap3(Object obj) throws Exception {
		if (obj == null) {
			return null;
		}

		Map<String, Object> map = new HashMap<String, Object>();

		Field[] declaredFields = obj.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			field.setAccessible(true);
			map.put(field.getName(), field.get(obj));
		}

		return map;
	}

}
