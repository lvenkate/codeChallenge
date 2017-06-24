package com.shutterfly.ltv;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import redis.clients.jedis.Jedis;

public class TopX{
	Map<String,Double> userLTV = new TreeMap<String, Double>();
	public void TopXSimpleLTVCustomers(int x , Imdb database){	
		Imdb dbConnection = database;
		try (Jedis jedis = dbConnection.getPool().getResource()) {
			Set<String> users = jedis.smembers("users");
			for(String user: users){
				Map<String, String> userOrders = jedis.hgetAll("order:"+user);
				float expenditure = 0.0f;
						for(String orderJSON: userOrders.values()){
					JsonObject order = (JsonObject)new JsonParser().parse(orderJSON);
					expenditure+=Double.parseDouble(order.get("total_amount").getAsString().split(" ")[0]);
				}		
				long visits = jedis.hlen("SITE_VISIT" + user);
				float moneySpent =0 ;
				if(visits!=0)
					moneySpent = (float)expenditure/visits;
				SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd':'HH:mm");
				Date systemTime = dateformat.parse(jedis.get("latestTimeStamp"),new ParsePosition(0));
				Date startTime = systemTime;
				for(String order: userOrders.values()){ 
					JsonObject jOOrder = (JsonObject)new JsonParser().parse(order);
					Date eventTime = dateformat.parse(jOOrder.get("event_time").getAsString(),new ParsePosition(0));
					if(eventTime.before(startTime)){
						startTime = eventTime;
					}
				}
				
				Set<String> userOrderDumps = jedis.smembers("orderDump:"+user);
				for(String dumpJSON: userOrderDumps){
					JsonObject order = (JsonObject)new JsonParser().parse(dumpJSON);
					Date eventTime = dateformat.parse(order.get("event_time").getAsString(),new ParsePosition(0));
					if(eventTime.before(startTime)){
						startTime = eventTime;
					}
				}
				float duration = (systemTime.getTime()-startTime.getTime())/(1000*60*60*24); 
			
				float valPerWeek = (moneySpent)*(visits/((duration==0)? 1 : duration));
				double topUser = 52*valPerWeek*10;
				userLTV.put(user, topUser);
				
			}
		}
		SortedSet<Map.Entry<String, Double>> sortedEntries = new TreeSet<Map.Entry<String, Double>>(
			        new Comparator<Map.Entry<String, Double>>() {
			            @Override public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
			                return e1.getValue().compareTo(e2.getValue()) != 0 ? e1.getValue().compareTo(e2.getValue()) : 1;
			            }
			        }
			    );
	    sortedEntries.addAll(userLTV.entrySet());
			 
		Iterator<Map.Entry<String, Double>> iterator = sortedEntries.iterator();
		for(int i =0;i<x && iterator.hasNext();i++){
			Map.Entry<String, Double> e = iterator.next();
			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
		              new FileOutputStream("./output/output.txt"), "utf-8"))) {
				writer.write("User::"+e.getKey()+"\\t"+e.getValue());
				writer.newLine(); 
				} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
				} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				} catch (IOException e1) {
				e1.printStackTrace();
				}
		}
	}
	
	
}