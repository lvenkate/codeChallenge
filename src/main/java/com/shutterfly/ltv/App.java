package com.shutterfly.ltv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class App 
{
		public static void main(String[] args){
			try(BufferedReader br = new BufferedReader(new FileReader("./input/input.txt"))) {
			    StringBuilder string = new StringBuilder();
			    String line = br.readLine();

			    while (line != null) {
			    	string.append(line);
			    	string.append(System.lineSeparator());
			        line = br.readLine();
			    }
			    String input = string.toString();
			   
			    JsonArray jsonArray = (JsonArray)(new JsonParser().parse(input));
			    Imdb database = new Imdb();    
			    for(JsonElement e: jsonArray){
			    	Ingest ingest = new Ingest();
			    	ingest.ingest(e.toString(),database); 
			    }
			    
			    TopX result = new TopX();
			    result.TopXSimpleLTVCustomers(3,database);
			}
			catch (FileNotFoundException e) {
				System.out.println("file exp::"+e);
			}
			catch (IOException e) {
				System.out.println("io exp::"+e);
			}
			
		}
}
