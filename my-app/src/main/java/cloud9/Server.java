package cloud9;

/**
 * Hello world!
 *
 */
import static spark.Spark.*;
import spark.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Server {

   public static void main(String[] args) {

   	setPort(80);     

      get(new Route("/q1") {
         @Override
         public Object handle(Request request, Response response) {         	
            return "cloud9,4897-8874-0242,"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
         }
      });

   }

}