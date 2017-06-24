package com.shutterfly.ltv;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
//A Redis connection for in-memory database access
public class Imdb {
	
	private JedisPoolConfig conf =null ;
	private JedisPool pool =null ;
	Imdb(){
		conf = new JedisPoolConfig();
		pool = new JedisPool(conf,"192.68.1.8");
		}
	
	public JedisPool getPool() {
		return pool;
	}
	public void setPool(JedisPool pool) {
		this.pool = pool;
	}
}
