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
	     	response.type("text/plain; charset=UTF-8");
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
					Result query = getFromHBase("tweets_q3", request.queryParams("userid"));
					byte[] userIds = query.getValue(column, qualifier);
					
					StringBuilder sb = new StringBuilder(userIds.length);
					sb.append(result);

					int offset = 0;							
					while(offset < userIds.length){
						sb.append(Bytes.toLong(userIds, offset));
						sb.append("\n");
						offset += 8;
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
					/*
					* NOTE: Change qualifier "c" if we change qualifier of tweets_q4 in HBase
					*/
					String query = getFromHBase("tweets_q4", "c", "c", request.queryParams("time"));					
					String[] tweetAndTexts = query.split("&;");
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
					response.type("text/plain; charset=UTF-8");
					//response.header("Content-Length", String.valueOf(result.length()));
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
				ResultScanner query = null; 
				try{
					if(isTimeStampValid(start_time) && isTimeStampValid(end_time)){
						query = scan("tweets_q5", place+start_time, place+end_time);
						current = query.next();
						while(current != null){ //For each row...
							//Get the value (a byte array of longs glued together)
							values = current.getNoVersionMap().get(column).get(qualifier);				
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
			 		query.close();
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
				long testMin = Long.parseLong(request.queryParams("userid_min"), 10);
				long testMax = Long.parseLong(request.queryParams("userid_max"), 10);
				if(testMin > MAX_UID){
					testMin = MAX_UID;
				}
				if(testMax > MAX_UID){
					testMax = MAX_UID;
				}

				byte[] userMinKey = Bytes.toBytes(testMin);				
				byte[] userMaxKey = Bytes.toBytes(testMax);
				byte[] userMinValue;
				byte[] userMaxValue;

				int count = 0;
				int sum = 4;
				int firstSum = 0;
				int secondSum = 0;			

				try{
					Result firstScan = scanOne("tweets_q6", userMinKey, userMaxKey);							
					Result secondScan = scanOne("tweets_q6",userMaxKey);
					
					userMinValue = firstScan.getNoVersionMap().get(column).get(qualifier);
					userMaxValue = secondScan.getNoVersionMap().get(column).get(qualifier);

					if(userMinValue.length != 0)
					{	
						//Read an integer from the byte[] starting at the 'sum' offset (4)
						firstSum = Bytes.toInt(userMinValue, sum);
					}
					if(userMaxValue.length != 0)
					{						
						if (Arrays.equals(secondScan.getRow(), userMaxKey)) {
							//If the userMax exists, then the value is its sum + count							
					 		secondSum = Bytes.toInt(userMaxValue, sum) + 
					 								Bytes.toInt(userMaxValue, count);
						}else{
							//If the userMax didn't exist, then the value is the next row's sum
							secondSum = Bytes.toInt(userMaxValue, sum);
						}						
					}
					result += String.valueOf(secondSum - firstSum) + "\n";											
					
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

	private static Result getFromHBase(String table, String row) throws IOException{
		Result r = null;		
		HTableInterface htable = pool.getTable(table);		
		try {
		// Use the table as needed, for a single operation and a single thread
			r = htable.get(new Get(row.getBytes()));			

		} finally {
			htable.close();
			return r;
		}	
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
	private static Result scanOne(String table, byte[] start) throws IOException{
		ResultScanner rs = scanFromHBase(table.getBytes(), start, null, true);
		Result r = rs.next();
		rs.close();
		return r;

	}

	private static Result scanOne(String table, byte[] start, byte[] stop) throws IOException{
		ResultScanner rs =  scanFromHBase(table.getBytes(), start, stop, true);
		Result r = rs.next();
		rs.close();
		return r;
	}

	private static ResultScanner scan(String table, String start, String stop) throws IOException{
		return scanFromHBase(table.getBytes(), start.getBytes(), stop.getBytes(), false);
	}
	/* Base Function */
	private static ResultScanner scanFromHBase(byte[] table, byte[] start, byte[] stop, boolean limit) throws IOException{		
		
		HTableInterface htable = pool.getTable(table);	
		ResultScanner scanResult = null;
		Scan scan = null;
		try {				
			if(stop != null){				
				scan = new Scan(start, Arrays.copyOf(stop, stop.length+1));	
			}	else {
				scan = new Scan(start);
			}

			if(limit){
				scan.setFilter(new PageFilter(1));	
				scan.setBatch(1);							
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
