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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.conf.Configuration;


public class Server {

	private static Configuration config;
	private static String[] serverArgs;

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	private static String heartbeat;

	private static final long MAX_UID = 2427052444L;

	public static void main(String[] args) {

		serverArgs = args.clone();	//Create a copy of the command line arguments that my handlers can access

		setPort(80);	//Listen on port 80 (which requires sudo)

		config = HBaseConfiguration.create();	//Create the HBaseConfiguration
		if(args.length > 0){
			config.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, args[0]);			
			config.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, args[1]);
		}

		heartbeat = "cloud9,4897-8874-0242,"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());

	  	get(new Route("/q1") {
	     @Override
	     public Object handle(Request request, Response response) {
	     	// String heartbeat = "cloud9,4897-8874-0242,"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());        		     	
	     	response.type("text/plain");
	     	response.header("Content-Length", String.valueOf(heartbeat.length()));
	      return heartbeat;
	     }
	  });

		get(new Route("/q2") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = heartbeat;			
				String q2key = request.queryParams("userid")+request.queryParams("tweet_time")
				String userIds = getFromHBase("tweets_q2", "c", "q", q2key);
				String[] ids = tweetIds.split(";");
				for (String id : ids)
				{
					result += id +"\n";
				}
				response.type("text/plain");
				response.header("Content-Length", String.valueOf(result.length()));
				return result;
			}
		});

		get(new Route("/q3") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = heartbeat;						
				String userIds = getFromHBase("tweets_q3", "c", "q", request.queryParams("userid"));
				String[] ids = tweetIds.split(";");
				for (String id : ids)
				{
					result += id +"\n";
				}
				response.type("text/plain");
				response.header("Content-Length", String.valueOf(result.length()));
				return result;
			}
		});

		get(new Route("/q4") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = "";
				//Q4 stuff here
				//request.queryParams("time")				
				
				response.type("text/plain");
				response.header("Content-Length", String.valueOf(result.length()));
				return result;
			}
		});

		get(new Route("/q5") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = "";
				//Q5 stuff here
				//request.queryParams("start_time")
				//request.queryParams("end_time")
				//request.queryParams("place")
				result += request.queryParams("start_time") + "\n"+
									request.queryParams("end_time") + "\n" +
									request.queryParams("place");
				response.type("text/plain");
				response.header("Content-Length", String.valueOf(result.length()));
				return result;
			}
		});

		get(new Route("/q6") {
			@Override
			public Object handle(Request request, Response response) {				        	
				String result = "";
				//Q6 stuff here
				//request.queryParams("userid_min")
				//request.queryParams("userid_max")		
				try{		
					long startSum = q6Scan("q6", "cf", 
										request.queryParams("userid_min").getBytes(), 
										request.queryParams("userid_max").getBytes());
					long endSum = q6Scan("q6", "cf", 
										request.queryParams("userid_max").getBytes(), 
										longToBytes(MAX_UID));
					//result = String.valueOf(endSum - startSum);
					result = heartbeat + "\n" + (endSum - startSum);
				}catch (IOException e){
		 			System.err.println(e.getMessage());
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
			 		result = scanFromHBase(request.params(":table"),
										request.params(":family"),
										request.params(":qualifier"),
										request.params(":start"),
										request.params(":stop"),
										true
										);
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

	private static String getFromHBase(String table, String family, String qualifier, String row) throws IOException{
		String result = "";
		//config.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
		//config.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);
		//HConnection connection = HConnectionManager.createConnection(config);	
		//HTableInterface htable = connection.getTable(table.getBytes());
		//connection.getTable() won't compile. Not sure why.
		HTable htable = new HTable(config, table.getBytes());
		try {
		// Use the table as needed, for a single operation and a single thread
			Result r = htable.get(new Get(row.getBytes()));
			result = new String(r.getValue(family.getBytes(), qualifier.getBytes()));

		} finally {
			htable.close();
			//connection.close();
		}
		return result;
	}

	private static String scanFromHBase(String table, String family, String qualifier, String start, String stop, boolean limit) throws IOException{		
		String output = "";
		HTable htable = new HTable(config, table.getBytes());
		try {		
			//byte[] startRow = ByteBuffer.allocate(4).putInt(Integer.parseInt(start)).array();
			//byte[] startRow = intToBytes(Integer.parseInt(start));
			//byte[] stopRow = intToBytes(Integer.parseInt(stop));
			//Scan scan = new Scan(startRow, stopRow);
			Scan scan = new Scan(start.getBytes(), stop.getBytes());
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

	private static long q6Scan(String table, String family, byte[] start, byte[] end) throws IOException{
		long output = 0;	
		HTable htable = new HTable(config, table.getBytes());
		
		try {
		// Use the table as needed, for a single operation and a single thread
			
			//Scan scan = new Scan(start, Arrays.copyOf(end, end.length+1));
			Scan scan = new Scan(start, end);
			
			//scan.setBatch(1);
			//scan.setFilter(new PageFilter(1));

			ResultScanner scanResult = htable.getScanner(scan);	
			Result result = scanResult.next();

			//int x = java.nio.ByteBuffer.wrap(bytes).getInt();
			long count = ByteBuffer.wrap(result.getValue(family.getBytes(), "count".getBytes())).getLong();
			long sum = ByteBuffer.wrap(result.getValue(family.getBytes(), "sum".getBytes())).getLong();

			System.out.println("Count:" + count);
			System.out.println("Sum:" + sum);
			if(Arrays.equals(result.getRow(), start))
				output = count + sum;
			else
				output = sum;

		} finally {
			htable.close();			
		}
		return output;
	}

	public static byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(x);
    return buffer.array();
	}

	public static byte[] intToBytes(int x) {
		return ByteBuffer.allocate(4).putInt(x).array();
	}

}