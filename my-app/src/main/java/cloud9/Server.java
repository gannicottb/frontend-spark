package cloud9;

/**
 * Frontend Server for Phase 3
 * Brandon Gannicott
 */

import static spark.Spark.*;
import spark.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.IOException;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;


public class Server {

	private static Configuration config;

	public static void main(String[] args) {

		setPort(80);     

		config = HBaseConfiguration.create();

	  get(new Route("/q1") {
	     @Override
	     public Object handle(Request request, Response response) {         	
	        return "cloud9,4897-8874-0242,"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
	     }
	  });

		get(new Route("/test") {
		 @Override
		 public Object handle(Request request, Response response) {   
		 		//String str= getFromHBase("test", "row1"); 
		 		String result = "";
		 		try {  
		 			result = getFromHBase("test", "cf", "a", "row1");
		 		} catch (IOException e){
		 			System.err.println(e.getMessage());
		 		} finally {
		 			return result;
		 		}
		 	// 	HTable htable = new HTable();
		 	// 	try {
		 	// 		htable = new HTable(config, "test");
		 	// 	} catch(IOException e){
				// 	System.err.println(e.getMessage());
				// }
		 		
				// try {
				// // Use the table as needed, for a single operation and a single thread
				// 	Result r = htable.get(new Get(new String("row1").getBytes()));
				// 	result = r.toString();
				// } catch(IOException e){
				// 	System.err.println(e.getMessage());
				// } finally {
				// 	htable.close();
				// 	//connection.close(); 
				// }  	
		    // return result;
		 }
		});

	}

	private static String getFromHBase(String table, String family, String qualifier, String row) throws IOException{
		String result = "";
		//HConnection connection = HConnectionManager.createConnection(config);
		//HTableInterface htable = connection.getTable(table.getBytes());
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


}