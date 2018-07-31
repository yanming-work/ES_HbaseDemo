package com.test.core.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import com.test.core.service.RedisService;
import com.test.core.util.FastJsonUtils;
@Component("redisService")
public class RedisServiceImpl implements RedisService {

	@Resource
	private RedisTemplate<String, Object> redisTemplate;
	@Resource
	private StringRedisTemplate stringRedisTemplate;

	@Override
	public boolean redisSet(String key, String value) {
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
				connection.set(serializer.serialize(key), serializer.serialize(value));
				return true;
			}
		});
		return result;
	}

	@Override
	public boolean redisSetTimeSeconds(String key, String value, long seconds) {
		// 由于设置的是10秒失效，十秒之内查询有结果，十秒之后返回为null
		// redisTemplate.opsForValue().set("name","tom",10, TimeUnit.SECONDS);
		boolean result = false;
		try {
			redisTemplate.opsForValue().set(key, value, seconds, TimeUnit.SECONDS);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public boolean redisSetTimeMinutes(String key, String value, long minutes) {
		// 由于设置的是10分钟失效，十分钟之内查询有结果，十分钟之后返回为null
		// redisTemplate.opsForValue().set("name","tom",10, TimeUnit.MINUTES);
		boolean result = false;
		try {
			redisTemplate.opsForValue().set(key, value, minutes, TimeUnit.MINUTES);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public boolean redisSetTimeHours(String key, String value, long hours) {
		// 由于设置的是10小时失效，十小时之内查询有结果，十小时之后返回为null
		// redisTemplate.opsForValue().set("name","tom",10, TimeUnit.HOURS);
		boolean result = false;
		try {
			redisTemplate.opsForValue().set(key, value, hours, TimeUnit.HOURS);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public boolean redisSetTimeDays(String key, String value, long days) {
		// 由于设置的是10天失效，十天之内查询有结果，十天之后返回为null
		// redisTemplate.opsForValue().set("name","tom",10, TimeUnit.DAYS);
		boolean result = false;
		try {
			redisTemplate.opsForValue().set(key, value, days, TimeUnit.DAYS);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public String redisGet(String key) {
		String result = redisTemplate.execute(new RedisCallback<String>() {
			@Override
			public String doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
				byte[] value = connection.get(serializer.serialize(key));
				return serializer.deserialize(value);
			}
		});
		return result;
	}

	@Override
	public boolean redisExpire(String key, long expire) {
		return redisTemplate.expire(key, expire, TimeUnit.SECONDS);
	}

	@Override
	public <T> boolean redisSetList(String key, List<T> list) {
		String value = FastJsonUtils.listToJsonStr(list);
		return redisSet(key, value);
	}

	@Override
	public <T> List<T> redisGetList(String key, Class<T> clz) {
		String json = redisGet(key);
		if (json != null) {
			List<T> list = FastJsonUtils.jsonArrStrToList(json, clz);
			return list;
		}
		return null;
	}

	/**
	 * 在key对应 list的头部添加字符串元素
	 * 
	 * @param key
	 * @param obj
	 * @return
	 */
	@Override
	public long redisLpush(String key, Object obj) {
		String value = FastJsonUtils.objectToJsonStr(obj);
		long result = redisTemplate.execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
				long count = connection.lPush(serializer.serialize(key), serializer.serialize(value));
				return count;
			}
		});
		return result;
	}

	/**
	 * 在key对应 list 的尾部添加字符串元素
	 * 
	 * @param key
	 * @param obj
	 * @return
	 */
	@Override
	public long redisRpush(String key, Object obj) {
		String value = FastJsonUtils.objectToJsonStr(obj);
		long result = redisTemplate.execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
				long count = connection.rPush(serializer.serialize(key), serializer.serialize(value));
				return count;
			}
		});
		return result;
	}

	@Override
	public String redisLpop(String key) {
		String result = redisTemplate.execute(new RedisCallback<String>() {
			@Override
			public String doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
				byte[] res = connection.lPop(serializer.serialize(key));
				return serializer.deserialize(res);
			}
		});
		return result;
	}

	/**
	 * 删除
	 */
	@Override
	public boolean redisDelete(String key) {
		boolean result = false;
		try {
			List<String> list = new ArrayList<String>();
			list.add(key);
			result = redisDeleteKeys(list);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 批量删除
	 */
	@Override
	public boolean redisDeleteKeys(List<String> keys) {
		boolean result = false;
		try {
			redisTemplate.delete(keys);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 保存map
	 */
	@Override
	public boolean redisMultiSet(Map<String, Object> maps) {
		boolean result = false;
		if (maps != null) {
			try {
				redisTemplate.opsForValue().multiSet(maps);
				result = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return result;
	}

	/**
	 * 批量获取
	 */
	@Override
	public Map<String, Object> redisMultiGetMap(List<String> keys) {
		Map<String, Object> maps = null;
		if (keys != null && keys.size() > 0) {
			List<Object> list = redisTemplate.opsForValue().multiGet(keys);

			if (list != null && list.size() == keys.size()) {
				maps = new HashMap<String, Object>();
				for (int i = 0; i < list.size(); i++) {
					maps.put(keys.get(i), list.get(i));
				}
			}
		}

		return maps;
	}

	/**
	 * 批量获取
	 */
	@Override
	public List<Object> redisMultiGetList(List<String> keys) {
		List<Object> list = redisTemplate.opsForValue().multiGet(keys);
		return list;
	}

	/**
	 * 为多个键分别设置它们的值，如果存在则返回false，不存在返回true
	 */
	@Override
	public boolean redisMultiSetIfAbsent(Map<String, Object> maps) {
		boolean result = false;
		if (maps != null) {
			try {
				redisTemplate.opsForValue().multiSetIfAbsent(maps);
				result = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return result;
	}

	/************** stringRedisTemplate ***********/

	@Override
	public boolean redisStringSet(String key, String value) {
		boolean result = false;
		try {
			stringRedisTemplate.opsForValue().set(key, value);
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public String redisStringGet(String key) {
		return stringRedisTemplate.opsForValue().get(key);
	}

	// 秒
	@Override
	public boolean redisStringSetTimeSeconds(String key, String value, long seconds) {
		boolean result = false;
		try {
			stringRedisTemplate.opsForValue().set(key, value, seconds, TimeUnit.SECONDS);// 向redis里存入数据和设置缓存时间
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 分
	@Override
	public boolean redisStringSetTimeMinutes(String key, String value, long minutes) {
		boolean result = false;
		try {
			stringRedisTemplate.opsForValue().set(key, value, minutes, TimeUnit.MINUTES);// 向redis里存入数据和设置缓存时间
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 小时
	@Override
	public boolean redisStringSetTimeHours(String key, String value, long hours) {
		boolean result = false;
		try {
			stringRedisTemplate.opsForValue().set(key, value, hours, TimeUnit.HOURS);// 向redis里存入数据和设置缓存时间
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 天
	@Override
	public boolean redisStringSetTimeDays(String key, String value, long days) {
		boolean result = false;
		try {
			stringRedisTemplate.opsForValue().set(key, value, days, TimeUnit.DAYS);// 向redis里存入数据和设置缓存时间
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 根据key获取过期时间
	@Override
	public long redisStringGetExpire(String key) {
		return stringRedisTemplate.getExpire(key);
	}

	// 根据key获取过期时间并换算成指定单位,Seconds
	@Override
	public long redisStringGetExpireSeconds(String key) {
		return stringRedisTemplate.getExpire(key, TimeUnit.SECONDS);
	}

	// 根据key获取过期时间并换算成指定单位,MINUTES
	@Override
	public long redisStringGetExpireMinutes(String key) {
		return stringRedisTemplate.getExpire(key, TimeUnit.MINUTES);
	}

	// 根据key获取过期时间并换算成指定单位,HOURS
	@Override
	public long redisStringGetExpireHours(String key) {
		return stringRedisTemplate.getExpire(key, TimeUnit.HOURS);
	}

	// 根据key获取过期时间并换算成指定单位,DAYS
	@Override
	public long redisStringGetExpireDays(String key) {
		return stringRedisTemplate.getExpire(key, TimeUnit.DAYS);
	}

	// 根据key删除缓存
	@Override
	public boolean redisStringDelete(String key) {
		boolean result = false;
		try {
			result = stringRedisTemplate.delete(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 检查key是否存在，返回boolean值
	@Override
	public boolean redisStringHasKey(String key) {
		boolean result = false;
		try {
			result = stringRedisTemplate.hasKey(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 设置过期时间SECONDS
	@Override
	public boolean redisStringExpireSeconds(String key, long seconds) {
		boolean result = false;
		try {
			stringRedisTemplate.expire(key, seconds, TimeUnit.SECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 设置过期时间MINUTES
	@Override
	public boolean redisStringExpireMinutes(String key, long minutes) {
		boolean result = false;
		try {
			stringRedisTemplate.expire(key, minutes, TimeUnit.MINUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 设置过期时间HOURS
	@Override
	public boolean redisStringExpireHours(String key, long hours) {
		boolean result = false;
		try {
			stringRedisTemplate.expire(key, hours, TimeUnit.HOURS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 设置过期时间DAYS
	@Override
	public boolean redisStringExpireDays(String key, long days) {
		boolean result = false;
		try {
			stringRedisTemplate.expire(key, days, TimeUnit.DAYS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 根据key查看集合中是否存在指定数据
	@Override
	public boolean redisStringIsMember(String key, String value) {
		boolean result = false;
		try {
			stringRedisTemplate.opsForSet().isMember(key, value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 根据key获取set集合
	@Override
	public Set<String> redisStringMembers(String key) {
		Set<String> result = null;
		try {
			result = stringRedisTemplate.opsForSet().members(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// 向指定key中存放set集合
	@Override
	public long redisStringAddSet(String key, String... values) {
		long result = 0;
		try {
			result = stringRedisTemplate.opsForSet().add(key, values);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}