import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.*;

public class App 
{
    public static void main( String[] args )
    {
    	MongoClient mongoClient = null;
    	try
    	{
    		mongoClient = new MongoClient("18.206.61.163", 27017);
    	}
    	catch(Exception e)
    	{
    		System.err.println("Cannot connect");
    	}
    	
		MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection<Document> collection = database.getCollection("list");
		collection.countDocuments();
		
		System.out.println(
				"1. Flights the occur on Christmas. \n" +
				"2. Number of flights from San Francisco to New York in (some interval). \n" +
				"3. Flights to the Memphis city.\n" +
				"4. Earliest or latest departure time from (a city) on (date).\n"+
				"5. Flights where arrival time is after 1800 or 6:00 pm.\n"+
				"6. Get the date of flights that have been cancelled.\n"+
				"7. Flights that are less than 120 minutes.\n" +
				"8. Flights where there is more than 1 stop.\n" +
				"9. Flights that use the same plane and there is a departure delay greater than 20 minutes.\n" +
				"10. Flights where the journey is greater than 200 miles.\n" +
				"11. Flights with no cancellations but is delayed by weather.\n" +
				"12. Flights where takeoff (taxi in) and touchdown (taxi off) is less than 60 minutes combined./n" +
				"13. Date of flights with multiple delays.\n"+
				"14. Flights that occur after 1pm and before 8pm.\n" +
				"15. Flights in December where depature delay is non-exsistent.\n"
				+ "\n"+"Enter in the query number you wish to execute: ");
		
		
    	
    	Scanner in = new Scanner(System.in);
    	
    	while(true)
    	{	
    		String input = in.next();
    		if(input.equals("EXIT"))
    		{
    			System.out.println("BYE");
    			mongoClient.close();
    			break;
    		}
    		else
    		{
    			System.out.println("INPUT: " + input);
    		}
    	}  
    }
    
    public void christmasFlights()
    {
    	//flights that occur on Christmas day
    }
    
    public void sfToNY()
    {
    	//number of flights from SF to NY in certain date interval
    }
    
    public void memphisFlights()
    {
    	//flights to a city named Memphis (state doesn't matter)
    }
    
    public void earlyLateDeparture()
    {
    	//earliest or latest flight on each day to certain location
    }
    
    public void arriveAfter6pm()
    {
    	//flights arriving after 6pm
    }
    
    public void cancelledDates()
    {
    	//dates of flights that have been cancelled
    }
    
    public void max2HRFlights()
    {
    	//flights to locations where the ride is less than 2 hours
    }
    
    public void manyStops()
    {
    	//flights that have more than 1 stops
    }
    
    public void samePlaneAndDelay()
    {
    	//flights on plane that have flown route multiple times and have a departure delay more than 20 minutes
    }
    
    public void miPlusTrip()
    {
    	//flights where the distance is greater than 200 miles
    }
    
    public void noCancellationButDelay()
    {
    	//flights that have no cancellation but have been weather delayed
    }
    
    public void minOffAndOn()
    {
    	//flights where take off and touch down time from and to terminal is less than 60 minutes combined
    }
    
    public void multDelays()
    {
    	//flights with multiple delays
    }
    
    public void after1Before8()
    {
    	//flights after 1pm and before 8 pm
    }
    
    public void noDecemberDelays()
    {
    	//flights in December with no departure delay time
    }
}
