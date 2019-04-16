import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.*;

/**
 * 
 * @author khoi
 *Apr 07, 2019 10:16:47 PM com.mongodb.diagnostics.logging.JULLogger log
INFO: Cluster created with settings {hosts=[ec2-3-92-197-163.compute-1.amazonaws.com:27021], mode=SINGLE, requiredClusterType=UNKNOWN, serverSelectionTimeout='30000 ms', maxWaitQueueSize=500}
Apr 07, 2019 10:16:47 PM com.mongodb.diagnostics.logging.JULLogger log
INFO: Cluster description not yet available. Waiting for 30000 ms before timing out
Apr 07, 2019 10:16:48 PM com.mongodb.diagnostics.logging.JULLogger log
INFO: Opened connection [connectionId{localValue:1}] to ec2-3-92-197-163.compute-1.amazonaws.com:27021
Apr 07, 2019 10:16:48 PM com.mongodb.diagnostics.logging.JULLogger log
INFO: Monitor thread successfully connected to server with description ServerDescription{address=ec2-3-92-197-163.compute-1.amazonaws.com:27021, type=SHARD_ROUTER, state=CONNECTED, ok=true, version=ServerVersion{versionList=[4, 0, 6]}, minWireVersion=0, maxWireVersion=7, maxDocumentSize=16777216, logicalSessionTimeoutMinutes=30, roundTripTimeNanos=82214163}
Apr 07, 2019 10:16:48 PM com.mongodb.diagnostics.logging.JULLogger log
INFO: Opened connection [connectionId{localValue:2}] to ec2-3-92-197-163.compute-1.amazonaws.com:27021
 */
public class App 
{
    public static void main( String[] args )
    {
    	MongoClient mongoClient = null;
    	try
    	{
    		mongoClient = new MongoClient("3.212.192.253", 27021); //change mongod.conf "bind_ip" to 0.0.0.0 and add custom TCP rule, port:27017 (or mongos port), and source: anywhere, to use public dns for connection 
    	}
    	catch(Exception e)
    	{
    		System.err.println("Cannot connect");
    	}
    	
		MongoDatabase database = mongoClient.getDatabase("project");
		MongoCollection<Document> collection = database.getCollection("list");
		System.out.println(collection.countDocuments());
		
		Document myDoc = collection.find(eq("FL_DATE", "12/31/09")).first();
		System.out.println(myDoc.toJson());
		
		/*Block<Document> printBlock = new Block<Document>() {
		     public void apply(final Document document) {
		         System.out.println(document.toJson());
		     }
		};*/

		//collection.find(and(gte("review_count", 10), lte("review_count", 20))).limit(3).forEach(printBlock);
		
		System.out.println(
				"1. Flights the occur on Christmas. \n" +
				"2. Number of flights from San Francisco to New York in (some interval). \n" +
				"3. Number of flights recieved by each airport.\n" +
				"4. Earliest or latest departure time from (a city) on (date).\n"+
				"5. Flights where arrival time is after 1800 or 6:00 pm.\n"+
				"6. Get the date of flights that have been cancelled.\n"+
				"7. Flights that are less than 120 minutes.\n" +
				"8. Flights where there is more than 1 stop.\n" +
				"9. Flights that use the same plane and there is a departure delay greater than 20 minutes.\n" +
				"10. Flights where the journey is greater than 200 miles.\n" +
				"11. Most popular flight destinations per quarter.\n" +
				"12. Flights where takeoff (taxi in) and touchdown (taxi off) is less than 60 minutes combined./n" +
				"13. Date of flights with multiple delays.\n"+
				"14. Flights that occur after 1pm and before 8pm.\n" +
				"15. Number of flights per quarter.\n"
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
    
    public void numberOfFlightsPerAirport()
    {
    	//number of flights arriving at each airport in 2009
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
    
    public void mostPopularDestPerQuarter()
    {
    	//most popular destination in each quarter
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
    
    public void flightsPerQuarter()
    {
    	//number of flights in each quarter
    }
}





