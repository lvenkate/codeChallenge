package com.shutterfly.ltv;
import redis.clients.jedis.Jedis;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

//Ingest(e, D)
//Given event e, update data D
public class Ingest {
	public void ingest(String event , Imdb database){	
		
				 Imdb dbConnection = database;
				 JsonObject jOEvent = (JsonObject)new JsonParser().parse(event);
				 if(jOEvent.has("event_time")){
					 try (Jedis jedis = dbConnection.getPool().getResource()) {
							if(!jedis.exists("latestTimeStamp")){
								jedis.set("latestTimeStamp", jOEvent.get("event_time").getAsString());
							}
							else{
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd':'HH:mm");
								Date prevStamp = dateFormat.parse(jedis.get("latestTimeStamp"),new ParsePosition(0));
								Date latest = dateFormat.parse(jOEvent.get("event_time").getAsString(),new ParsePosition(0));
								if(latest.after(prevStamp)){
									jedis.set("latestTimeStamp", jOEvent.get("event_time").getAsString());
								}
							}
						}
					 //Check for new customer entry and make an update
					 if(jOEvent.get("type").getAsString().equals("CUSTOMER")){
						 String id = jOEvent.get("key").getAsString();
							try (Jedis jedis = dbConnection.getPool().getResource()) {
								jedis.sadd("users",id);
								if(jOEvent.get("verb").getAsString().equals("NEW")) {
									if(!jedis.exists("user:"+id)){
										jedis.set("user:"+id, event.toString());
									}
									else{
										jedis.sadd("dump:"+id, event.toString());
									}				
								}
								else{
									if(jedis.exists("user:"+id)){
									
										JsonObject oldEvent = (JsonObject)new JsonParser().parse(jedis.get("user:"+id));
										SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd':'HH:mm");
										Date previousDate = dateFormat.parse(oldEvent.get("event_time").getAsString(),new ParsePosition(0));
										Date currentDate = dateFormat.parse(jOEvent.get("event_time").getAsString(),new ParsePosition(0));
										if(currentDate.after(previousDate)){
											jedis.sadd("dump:"+id, jedis.get("user:"+id));
											jedis.set("user:"+id, event.toString());
										}
										else jedis.sadd("dump:"+id, event.toString());
									}
									else jedis.set("user:"+id, event.toString());  
								}		
							}
						
					 }
					 //Update the order for a user
					 else if(jOEvent.get("type").getAsString().equals("ORDER")){
						 
						 try (Jedis jedis = dbConnection.getPool().getResource()) {
								if(!jedis.exists("latestTimeStamp")){
									jedis.set("latestTimeStamp", jOEvent.get("event_time").getAsString());
								}
								else{
									SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd':'HH:mm");
									Date dPrevStamp = sdf.parse(jedis.get("latestTimeStamp"),new ParsePosition(0));
									Date latest = sdf.parse(jOEvent.get("event_time").getAsString(),new ParsePosition(0));
									if(latest.after(dPrevStamp)){
										jedis.set("latestTimeStamp", jOEvent.get("event_time").getAsString());
									}
								}
							}
						 String id = jOEvent.get("key").getAsString();
							String customerID = jOEvent.get("customer_id").getAsString();
							try (Jedis jedis = dbConnection.getPool().getResource()) {
								jedis.sadd("orders",id);
								if(jOEvent.get("verb").getAsString().equals("NEW")){
									if(!jedis.hexists("order:"+customerID, id)){
										jedis.hset("order:"+customerID,id, jOEvent.toString());
									}
									else 
										jedis.sadd("orderDump:"+customerID,jOEvent.toString());
								}
								else{
									if(jedis.hexists("order:"+customerID,id)){
										JsonObject oldEvent = (JsonObject)new JsonParser().parse(jedis.hget("order:"+customerID,id));
										SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd':'HH:mm");
								
										Date previous = dateFormat.parse(oldEvent.get("event_time").getAsString(),new ParsePosition(0));
										Date current = dateFormat.parse(jOEvent.get("event_time").getAsString(),new ParsePosition(0));
										if(current.after(previous)){
											jedis.sadd("orderDump:"+customerID, jedis.hget("order:"+customerID,id));
											jedis.hset("order:"+customerID, id,event.toString());
										}
										else jedis.sadd("orderDump:"+customerID, event.toString());
									}
									else jedis.hset("order:"+customerID,id, event.toString());  
								}
								
							}
								 }
					 else{
							String id = jOEvent.get("key").getAsString();
							String customerID = jOEvent.get("customer_id").getAsString();
							String type = jOEvent.get("type").getAsString();
							try (Jedis jedis = dbConnection.getPool().getResource()) {
								jedis.sadd(type+"s",id);
								jedis.hset(type+":"+customerID,id, event.toString());
							}	
                    }
				 }
		}
	}


