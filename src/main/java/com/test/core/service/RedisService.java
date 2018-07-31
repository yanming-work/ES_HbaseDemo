package com.test.core.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisService {
	/*************redisTemplate**********/
	 public boolean redisSet( String key,  String value);
	 //秒
	 public boolean redisSetTimeSeconds( String key,  String value,long seconds);
	 //分
	 public boolean redisSetTimeMinutes( String key,  String value,long minutes);
	 //小时
	 public boolean redisSetTimeHours( String key,  String value,long hours);
	 //天
	 public boolean redisSetTimeDays( String key,  String value,long days);
	 public String redisGet( String key);
	 public boolean redisExpire( String key, long expire);
	 public <T> boolean redisSetList(String key, List<T> list);
	 public <T> List<T> redisGetList(String key, Class<T> clz);
	 /**
	  * 在key对应 list的头部添加字符串元素
	  * @param key
	  * @param obj
	  * @return
	  */
	 public long redisLpush( String key, Object obj);
	 /**
	  * 在key对应 list 的尾部添加字符串元素
	  * @param key
	  * @param obj
	  * @return
	  */
	 public long redisRpush( String key, Object obj);
	 public String redisLpop( String key);
	 public boolean redisDeleteKeys(List<String> keys);
	 public boolean redisDelete(String key);
	 public boolean redisMultiSet (Map<String,Object> maps);
	 public Map<String,Object>  redisMultiGetMap (List<String> keys);
	 public List<Object>  redisMultiGetList (List<String> keys);
	 public boolean redisMultiSetIfAbsent(Map<String,Object> maps);
	 
	 /***stringRedisTemplate***/
	 public String redisStringGet( String key);
	 public boolean redisStringSet( String key,  String value);
	 //秒
	 public boolean redisStringSetTimeSeconds( String key,  String value,long seconds);
	 //分
	 public boolean redisStringSetTimeMinutes( String key,  String value,long minutes);
	 //小时
	 public boolean redisStringSetTimeHours( String key,  String value,long hours);
	 //天
	 public boolean redisStringSetTimeDays( String key,  String value,long days);
	 
	//根据key获取过期时间并换算成指定单位
	 public long redisStringGetExpire(String key);
	 public long redisStringGetExpireSeconds(String key);
	 public long redisStringGetExpireMinutes(String key);
	 public long redisStringGetExpireHours(String key);
	 public long redisStringGetExpireDays(String key);
	//根据key删除缓存
	 public boolean redisStringDelete(String key);
	//检查key是否存在，返回boolean值
	 public boolean redisStringHasKey(String key);
	//设置过期时间
	 public boolean redisStringExpireSeconds(String key,long seconds);
	//设置过期时间
	 public boolean redisStringExpireMinutes(String key,long minutes);
	//设置过期时间
	 public boolean redisStringExpireHours(String key,long hours);
	//设置过期时间
	 public boolean redisStringExpireDays(String key,long days);
	//根据key查看集合中是否存在指定数据
	 public boolean redisStringIsMember(String key,String value);
	//根据key获取set集合
	 public Set<String>  redisStringMembers(String key );
	//向指定key中存放set集合
	 public long  redisStringAddSet(String key , String... values);
	 
		
	 
	 //redisTemplate.opsForValue();//操作字符串
	 //redisTemplate.opsForHash();//操作hash
	 //redisTemplate.opsForList();//操作list
	 //redisTemplate.opsForSet();//操作set
	 //redisTemplate.opsForZSet();//操作有序set

	 //linsert,在key对应 list 的特定位置之前或之后添加字符串元素
	 //lset,设置list中指定下标的元素值(下标从0开始)
	 //lrem,从key对应 list 中删除 count 个和 value 相同的元素。
	 
	 

	 //stringRedisTemplate.boundValueOps("test").increment(-1);//val做-1操作
	 //stringRedisTemplate.boundValueOps("test").increment(1);//val +1

	

	 
}
