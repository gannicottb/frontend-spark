package cloud9;

/**
 * Frontend Server for Phase 3
 * Brandon Gannicott
 */

import static spark.Spark.*;
import spark.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.nio.ByteBuffer;
import java.lang.StringBuilder;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;


public class Server {

	private static Configuration config;
	private static String[] serverArgs;
	private static HTablePool pool;
	
	private static String teamHeader = "cloud9,4897-8874-0242";

	private static final long MAX_UID = 2427052444L;
	private static byte[] column;
	private static byte[] qualifier;

	public static void main(String[] args) {

		serverArgs = args.clone();	//Create a copy of the command line arguments that my handlers can access

		setPort(80);	//Listen on port 80 (which requires sudo)

		column = "c".getBytes();
		qualifier = "q".getBytes();

		config = HBaseConfiguration.create();	//Create the HBaseConfiguration
		if(args.length > 0){
			config.set("hbase.zookeeper.quorum", args[0]+":2181");			
			config.set("hbase.zookeeper.property.clientPort", "2181");
			config.set("hbase.zookeeper.dns.nameserver", args[0]);
			config.set("hbase.regionserver.port", "60020");
			config.set("hbase.master", args[0]+":9000");
		}
			pool = new HTablePool(config, 50);


	  	get(new Route("/q1") {
	     @Override
	     public Object handle(Request request, Response response) {
	     	String heartbeat = teamHeader+","+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
	     	response.type("text/plain");
	     	response.header("Content-Length", String.valueOf(heartbeat.length()));
	      return heartbeat;
	     }
	  });

		get(new Route("/q2") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = teamHeader+"\n";			
				String q2key = request.queryParams("userid")+request.queryParams("tweet_time");
				try{
					String tweetIds = getFromHBase("tweets_q2", "c", "q", q2key);
					String[] ids = tweetIds.split(";");
					StringBuilder sb = new StringBuilder(ids.length*18);
					sb.append(result);					
					for (String id : ids){
						sb.append(id);
						sb.append("\n");
					}
					result = sb.toString();
				} catch (Exception e){
			 			e.printStackTrace();
			 	} finally {
					response.type("text/plain");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
				}
			}
		});

		get(new Route("/q3") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = teamHeader+"\n";		
				try{				
					String userIds = getFromHBase("tweets_q3", "c", "q", request.queryParams("userid"));
					String[] ids = userIds.split(";");
					StringBuilder sb = new StringBuilder(ids.length*18);
					sb.append(result);					
					for (String id : ids){
						sb.append(id);
						sb.append("\n");
					}
					result = sb.toString();
					} catch (Exception e){
			 			e.printStackTrace();
		 		} finally {
					response.type("text/plain");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
				}
			}
		});

		get(new Route("/q4") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = teamHeader+"\n";
				try{				
					String query = getFromHBase("tweets_q4", "c", "q", request.queryParams("time"));					
					String[] tweetAndTexts = query.split("\t");
					StringBuilder sb = new StringBuilder(tweetAndTexts.length*150);
					sb.append(result);					
					for (String tweetAndText : tweetAndTexts){
						sb.append(tweetAndText);
						sb.append("\n");
					}
					result = sb.toString();					
					} catch (Exception e){
			 			e.printStackTrace();
			 	} finally {
					response.type("text/plain");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
				}												
			}
		});

		get(new Route("/q5") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = teamHeader+"\n";				
				String place = request.queryParams("place");
				String start_time = request.queryParams("start_time");
				String end_time = request.queryParams("end_time");			
				Result current;						
				byte[] values;
				TreeSet<Long> sorted = new TreeSet();
				try{
					if(isTimeStampValid(start_time) && isTimeStampValid(end_time)){
						ResultScanner query = scan("tweets_q5", place+start_time, place+end_time);
						current = query.next();
						while(current != null){ //For each row...
							//Get the value (a byte array of longs glued together)
							//DEBUG//values = current.getNoVersionMap().get(column).get(qualifier);
							byte[] inDB = new byte[24];
							Bytes.putLong(inDB, 0, 1L);
							Bytes.putLong(inDB, 8, 2L);
							Bytes.putLong(inDB, 16, 3L);

							values = inDB;
							int offset = 0;							
							while(offset < values.length){
								sorted.add(Bytes.toLong(values, offset));	//Add as longs to the TreeSet
								offset += 8;
							}		
							current = query.next();				
						}
						//Use StringBuilder to build the concatenated response
						StringBuilder sb = new StringBuilder(sorted.size()*18);
						sb.append(result);
						for(Long id : sorted){
							sb.append(id);
							sb.append("\n");
						}
						result = sb.toString();
					}
				}catch (Exception e){
			 			e.printStackTrace();
			 	} finally {
					response.type("text/plain");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
				}
			}
		});

		get(new Route("/q6") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = teamHeader+"\n";
				String userMin = request.queryParams("userid_min");
				String userMax = request.queryParams("userid_max");
				
				byte[] count = "c1".getBytes();
				byte[] sum = "c2".getBytes();
				int firstSum = 0;
				int secondSum = 0;					
				NavigableMap<byte[],NavigableMap<byte[],byte[]>> first;
				NavigableMap<byte[],NavigableMap<byte[],byte[]>> second;
				try{
					Result firstScan = scanOne("tweets_q6", userMin, userMax);					
					Result secondScan = scanOne("tweets_q6",userMax);
					first = firstScan.getNoVersionMap();								
					second = secondScan.getNoVersionMap();

					if(!first.isEmpty())
					{	
						firstSum = Bytes.toInt(first.get(column).get(sum));
					}
					if(!second.isEmpty())
					{
						if (new String(secondScan.getRow()).equals(userMax)) {
					 		secondSum = Bytes.toInt(second.get(column).get(sum)) + 
					 								Bytes.toInt(second.get(column).get(count));
						}else{
							secondSum = Bytes.toInt(second.get(column).get(sum));
						}						
					}
					result += String.valueOf(secondSum - firstSum);				

					/*This code works, just so ya know*/
					// result += Bytes.toString(first.get("cf".getBytes()).get("count".getBytes()))+"\n";
					// result += Bytes.toString(first.get("cf".getBytes()).get("sum".getBytes()))+"\n";
					// result += Bytes.toString(second.get("cf".getBytes()).get("count".getBytes()))+"\n";
					// result += Bytes.toString(second.get("cf".getBytes()).get("sum".getBytes()))+"\n";					
					
				}catch (Exception e){
		 			e.printStackTrace();
		 		} finally {
		 			response.type("text/plain");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
		 		}		 	
			}
		});

		get(new Route("/scan/:table/:family/:qualifier/:start/:stop") {
		 @Override
		 public Object handle(Request request, Response response) { 
		 		String result = "";
		 		try{
			 		result = scanFromHBaseDebug(request.params(":table"),
										request.params(":family"),
										request.params(":qualifier"),
										request.params(":start"),
										request.params(":stop"),
										false);

			 	} catch (IOException e){
			 			System.err.println(e.getMessage());
			 		} finally {
			 		response.type("text/plain");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
				}
			}
		});


		get(new Route("/get/:table/:family/:qualifier/:row") {
		 @Override
		 public Object handle(Request request, Response response) {   
		 		String result = "";
		 		try {  
		 			result = getFromHBase(request.params(":table"), 
										request.params(":family"),
										request.params(":qualifier"),
										request.params(":row"));
		 		} catch (IOException e){
		 			System.err.println(e.getMessage());
		 		} finally {
		 			return result;
		 		}		 	
		 }
		});

		get(new Route("/raw/:table/:row") {
		 @Override
		 public Object handle(Request request, Response response) {   
		 		String result = "";
		 		try {  
		 			result = rawFromHBase(request.params(":table"),										
										request.params(":row"));
		 		} catch (IOException e){
		 			System.err.println(e.getMessage());
		 		} finally {
		 			return result;
		 		}		 	
		 }
		});

		get(new Route("/args") {
		 @Override
		 public Object handle(Request request, Response response) {   
		 		String result = "";
		 		for(String s : serverArgs)
		 		{
		 			result += s;
		 		}	
		 		return result;
		 }
		});
	}

	private static String rawFromHBase(String table, String row) throws IOException{
		String result = "";			
		HTableInterface htable = pool.getTable(table);		
		try {
			Result r = htable.get(new Get(row.getBytes()));
			//KeyValue[] result = r.raw();
			for(KeyValue kv : r.raw()){
				result += new String(kv.getValue());
			}

		} finally {
			htable.close();			
		}
		return result;
	}

	private static String getFromHBase(String table, String family, String qualifier, String row) throws IOException{
		String result = "";			
		HTableInterface htable = pool.getTable(table);		
		try {
		// Use the table as needed, for a single operation and a single thread
			Result r = htable.get(new Get(row.getBytes()));
			result = new String(r.getValue(family.getBytes(), qualifier.getBytes()));

		} finally {
			htable.close();
			
		}
		return result;
	}

	private static String scanFromHBaseDebug(String table, String family, String qualifier, String start, String stop, boolean limit) throws IOException{		
		String output = "";
		HTableInterface htable = pool.getTable(table);	
		try {					
			//Scan scan = new Scan(startRow, stopRow);
			byte[] stopAsBytes = stop.getBytes();
			byte[] stopPlusZero = Arrays.copyOf(stopAsBytes, stopAsBytes.length+1);
			// Scan scan = new Scan(start.getBytes(), stop.getBytes());
			Scan scan = new Scan(start.getBytes(), stopPlusZero);
			//Set some options
			if(limit){				
				scan.setFilter(new PageFilter(1));
			}					

			ResultScanner scanResult = htable.getScanner(scan);				
			Result result = scanResult.next();
			output += new String(result.getValue(family.getBytes(), 
									qualifier.getBytes()));			
			while(result != null){
				result = scanResult.next();	
				output += "\n"+ new String(result.getValue(family.getBytes(), 
									qualifier.getBytes()));						
			}	

		}catch (IOException e){
 			System.err.println(e.getMessage());
 		} finally {
			htable.close();			
			return output;
		}		
	}
	/* Interfaces to scanFromHBase */
	private static Result scanOne(String table, String start) throws IOException{
		return scanFromHBase(table, start, null, true).next();
	}

	private static Result scanOne(String table, String start, String stop) throws IOException{
		return scanFromHBase(table, start, stop, true).next();
	}

	private static ResultScanner scan(String table, String start, String stop) throws IOException{
		return scanFromHBase(table, start, stop, false);
	}
	/* Base Function */
	private static ResultScanner scanFromHBase(String table, String start, String stop, boolean limit) throws IOException{		
		
		HTableInterface htable = pool.getTable(table);	
		ResultScanner scanResult = null;
		Scan scan = null;
		try {				
			if(stop != null){
				byte[] stopAsBytes = stop.getBytes();			
				scan = new Scan(start.getBytes(), Arrays.copyOf(stopAsBytes, stopAsBytes.length+1));	
			}	else {
				scan = new Scan(start.getBytes());
			}

			if(limit){
				scan.setFilter(new PageFilter(1));
			}
			scanResult = htable.getScanner(scan);	
		} catch (Exception e){
			e.printStackTrace();			
		}	finally {
			htable.close();
			return scanResult;
		}
	}	

	public static boolean isTimeStampValid(String inputString)
	{ 		
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try{
       format.parse(inputString);
       return true;
    }
    catch(ParseException e)
    {
        return false;
    }
	}

}
