package cloud9;

/**
 * Frontend Server for Phase 3
 * Brandon Gannicott
 */

import static spark.Spark.*;
import spark.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.StringBuilder;

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

	public static void main(String[] args) {

		serverArgs = args.clone();	//Create a copy of the command line arguments that my handlers can access

		setPort(80);	//Listen on port 80 (which requires sudo)

		config = HBaseConfiguration.create();	//Create the HBaseConfiguration
		if(args.length > 0){
			config.set("hbase.zookeeper.quorum", args[0]+":2181");			
			config.set("hbase.zookeeper.property.clientPort", "2181");
			config.set("hbase.zookeeper.dns.nameserver", args[0]);
			config.set("hbase.regionserver.port", "60020");
			config.set("hbase.master", args[0]+":9000");
		}
			pool = new HTablePool(config, 25);


	  	get(new Route("/q1") {
	     @Override
	     public Object handle(Request request, Response response) {
	     	String heartbeat = heartbeat();
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
				String value;
				TreeSet<Long> sorted = new TreeSet();
				try{
					ResultScanner query = scanFromHBase("tweets_q5", place+start_time, place+end_time);
					current = query.next();
					while(current != null){ //For each row...
						//Get the value (a semicolon delimited list of ids)
						value = Bytes.toString(current.getNoVersionMap().get("c".getBytes()).get("q".getBytes()));
						String[] splitted = value.split(";"); //Split them on ;
						for(String s : splitted){
							sorted.add(Long.parseLong(s, 10));	//Add as longs to the TreeSet
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
				long firstSum = 0;
				long secondSum = 0;		
				KeyValue[] firstKV;
				KeyValue[] secondKV;
				NavigableMap<byte[],NavigableMap<byte[],byte[]>> first;
				NavigableMap<byte[],NavigableMap<byte[],byte[]>> second;
				try{

					Result firstScan = q6Scan("q6", 
																request.queryParams("userid_min"), 
																request.queryParams("userid_max"));		
					// firstKV = firstScan.raw();
					first = firstScan.getNoVersionMap();
					Result secondScan = q6Scan("q6", 
																request.queryParams("userid_max") 
																);					
					// secondKV = secondScan.raw();
					second = secondScan.getNoVersionMap();
					// if(firstKV.length == 2)
					// {	
					// 	firstSum = ByteBuffer.wrap(firstKV[1].getValue()).getLong();
					// }
					// if(secondKV.length == 2)
					// {
					// 	if (new String(secondScan.getRow()).equals(request.queryParams("userid_max"))){
					// 		secondSum = ByteBuffer.wrap(secondKV[0].getValue()).getLong() + 
					// 								ByteBuffer.wrap(secondKV[1].getValue()).getLong();
					// 	}else{
					// 		secondSum = ByteBuffer.wrap(secondKV[1].getValue()).getLong();
					// 	}						
					// }
					//result += String.valueOf(secondSum - firstSum);
					// result += ByteBuffer.wrap(firstKV[0].getValue()).getChar();
					// result += ByteBuffer.wrap(firstKV[1].getValue()).getChar();
					// result += ByteBuffer.wrap(secondKV[0].getValue()).getChar();
					// result += ByteBuffer.wrap(secondKV[1].getValue()).getChar();
					
					// result += new String(firstKV[0].getValue())+"\n";
					// result += new String(firstKV[1].getValue())+"\n";
					// result += new String(secondKV[0].getValue())+"\n";
					// result += new String(secondKV[1].getValue())+"\n";

					// result += Bytes.toString(firstKV[0].getValue())+"\n";
					// result += Bytes.toString(firstKV[1].getValue())+"\n";
					// result += Bytes.toString(secondKV[0].getValue())+"\n";
					// result += Bytes.toString(secondKV[1].getValue())+"\n";

					result += Bytes.toString(first.get("cf".getBytes()).get("count".getBytes()))+"\n";
					result += Bytes.toString(first.get("cf".getBytes()).get("sum".getBytes()))+"\n";
					result += Bytes.toString(second.get("cf".getBytes()).get("count".getBytes()))+"\n";
					result += Bytes.toString(second.get("cf".getBytes()).get("sum".getBytes()))+"\n";
					
					
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
			//scan.setReversed(reversed);

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

	private static ResultScanner scanFromHBase(String table, String start, String stop) throws IOException{		
		
		HTableInterface htable = pool.getTable(table);	
		ResultScanner scanResult = null;
		try {				
			byte[] stopAsBytes = stop.getBytes();
			//byte[] stopPlusZero = Arrays.copyOf(stopAsBytes, stopAsBytes.length+1);
			Scan scan = new Scan(start.getBytes(), Arrays.copyOf(stopAsBytes, stopAsBytes.length+1));		
			scanResult = htable.getScanner(scan);	
		} catch (Exception e){
			e.printStackTrace();			
		}	finally {
			htable.close();
			return scanResult;
		}
	}		

	private static Result q6Scan(String table, String start, String end) throws IOException{
		Result result;
		HTableInterface htable = pool.getTable(table);	
		try {
			Scan scan = new Scan(start.getBytes(), end.getBytes());			
			scan.setFilter(new PageFilter(1));
			ResultScanner scanResult = htable.getScanner(scan);	
			result = scanResult.next();			
			
			// long count = ByteBuffer.wrap(result.getValue(family.getBytes(), "count".getBytes())).getLong();
			// long sum = ByteBuffer.wrap(result.getValue(family.getBytes(), "sum".getBytes())).getLong();

			// System.out.println("Count:" + count);
			// System.out.println("Sum:" + sum);
			// if(Arrays.equals(result.getRow(), start))
			// 	output = count + sum;
			// else
			// 	output = sum;

		} finally {
			htable.close();			
		}
		return result;
	}

	private static Result q6Scan(String table, String start) throws IOException{
		Result result;
		HTableInterface htable = pool.getTable(table);	
		try {
			Scan scan = new Scan(start.getBytes());			
			scan.setFilter(new PageFilter(1));
			ResultScanner scanResult = htable.getScanner(scan);	
			result = scanResult.next();			
			
			// long count = ByteBuffer.wrap(result.getValue(family.getBytes(), "count".getBytes())).getLong();
			// long sum = ByteBuffer.wrap(result.getValue(family.getBytes(), "sum".getBytes())).getLong();

			// System.out.println("Count:" + count);
			// System.out.println("Sum:" + sum);
			// if(Arrays.equals(result.getRow(), start))
			// 	output = count + sum;
			// else
			// 	output = sum;

		} finally {
			htable.close();			
		}
		return result;
	}

	public static byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(x);
    return buffer.array();
	}

	public static byte[] intToBytes(int x) {
		return ByteBuffer.allocate(4).putInt(x).array();
	}

	public static String heartbeat(){
		return teamHeader+","+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
	}

}
