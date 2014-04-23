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
					String tweetIds = new String(getFromHBase("tweets_q2", q2key).getValue(column, qualifier));					
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
					response.type("text/plain; charset=UTF-8");
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
					response.type("text/plain; charset=UTF-8");
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
					String query = new String(getFromHBase("tweets_q4", request.queryParams("time")).getValue(column, "c".getBytes()));					
					String[] tweetAndTexts = query.split("&;");
					StringBuilder sb = new StringBuilder(tweetAndTexts.length*150);
					sb.append(result);					
					for (String tweetAndText : tweetAndTexts){
						//Idea: sb.append (tweetAndText.getBytes("UTF-8"));
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
					response.type("text/plain; charset=UTF-8");
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
					Result firstScan = scanOne("tweets_q6", userMinKey);							
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
		 			response.type("text/plain; charset=UTF-8");
					response.header("Content-Length", String.valueOf(result.length()));
					return result;
		 		}		 	
			}
		});		
	}

	/* HBase Query Functions */

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
