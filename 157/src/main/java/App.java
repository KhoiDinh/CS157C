import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.AggregateIterable;
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
import com.mongodb.util.JSON;

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
public class App {
    public static void main( String[] args ) {
    		MongoClient mongoClient = null;
	   
    		try {
	    		mongoClient = new MongoClient("3.212.192.253", 27021); //change mongod.conf "bind_ip" to 0.0.0.0 and add custom TCP rule, port:27017 (or mongos port), and source: anywhere, to use public dns for connection 
	    	}
	    	catch(Exception e) {
	    		System.err.println("Cannot connect");
	    	}
	    	
		MongoDatabase database = mongoClient.getDatabase("project");
		MongoCollection<Document> collection = database.getCollection("list");
		//System.out.println(collection.countDocuments());
		
	    	Scanner in = new Scanner(System.in);
	    	
	    	while(true)	{	
	    		// Menu
	    		System.out.println("\n" +
	    				"1. Flights the occur on Christmas from 12/23/09 to 12/25/09. \n" +
	    				"2. Number of flights from San Francisco, CA to New York, NY in 2009. \n" +
	    				"3. Number of flights recieved by a Chicago, IL in each month.\n" +
	    				"4. Earliest and latest departure time from San Jose, CA on 1/2/09.\n"+
	    				"5. Flights where arrival time is after 1600 or 4:00 pm in Detroit, MI on 4/1/09.\n"+
	    				"6. Get the information of flights that have been cancelled.\n"+
	    				"7. Flights that are less than 120 minutes.\n" +
	    				"8. Flights where there is more than 1 stop.\n" +
	    				"9. Flights where the journey is greater than 200 miles.\n" +
	    				"10. Flights that occur after 1pm and before 8pm.\n" +
	    				"11. Most popular flight destinations in 2009.\n" +
	    				"12. Most popular week days in 2009.\n" +
	    				"13. Number of flights per quarter.\n" +
	    				"14. Top ten flights that had most arrival delays in 2009.\n"+
	    				"15. Flights that have arrival delay time less than departure delay time.\n"
	    				+ "\n"+"Enter in the query number you wish to execute: ");
	    		String input = in.next();
	    		
	    		if(input.equals("EXIT")){
	    			System.out.println("BYE");
	    			mongoClient.close();
	    			break;
	    		}
	    		else{
	    			System.out.println("INPUT: " + input);
	    			switch(input) {
	    				case "1": christmasFlights(collection);
	    						  break;
	    				case "2": startToFinDest(collection);
	    						  break;
	    				case "3": numberOfFlightsPerAirport(collection);
	    						  break;
	    				case "4": earlyLateDeparture(collection);
	    					      break;
	    				case "5": arriveAfter6pm(collection);
						  	  break;
	    				case "6": cancelledDates(collection);
	    					      break;
	    				case "7": max2HRFlights(collection);
	    						  break;
	    				case "8": manyStops(collection);
	    						  break;
	    				case "9": miPlusTrip(collection);
	    						  break;
	    				case "10": after1Before8(collection);
	    						  break;
	    				case "11": mostPopularDestPerQuarter(collection);
	    						  break;
	    				case "12": minOffAndOn(collection);
	    						  break;
	    				case "13": flightsPerQuarter(collection);
	    						   break;
	    				case "14": multDelays(collection);
	    						   break;
	    				case "15": arrDellessdepDel(collection);
	    						   break;
	    					
	    			}
	    		}
	    	}  
	    	in.close();
    }
    
    /*
     * Function One:
     * 
     * Retrieves information of all flights that occurred during
     * the Christmas season from 12/23/09 to 12/25/09
     */
    public static void christmasFlights(MongoCollection<Document> collection) {
    	    		
    		// Defined dates of Christmas season duration
    		List<String> xmasDates = new ArrayList<String>();
    		xmasDates.add("12/23/09");
    		xmasDates.add("12/24/09");
    		xmasDates.add("12/25/09");
    		
    		FindIterable<Document> xmasIt = collection.find(in("FL_DATE", xmasDates)).projection(Projections.include(
    				"FL_DATE",
    				"OP_UNIQUE_CARRIER",
    				"TAIL_NUM",
    				"OP_CARRIER_FL_NUM",
    				"ORIGIN_CITY_NAME",
    				"DEST_CITY_NAME",
    				"DEP_TIME",
    				"ARR_TIME",
    				"DISTANCE"
    				)).limit(10);
    		
    		for(Document doc: xmasIt) {
    			System.out.println(doc.toJson());
    		}
    }
    
    /*
     * Function Two:
     * 
     * Retrieves information of all flights that traveled from
     * a given city to another given city in 2009
     */
    public static void startToFinDest(MongoCollection<Document> collection) {
		
		FindIterable<Document> sToFDestIt = collection.find(and(eq("ORIGIN_CITY_NAME", "San Francisco, CA"),eq("DEST_CITY_NAME", "New York, NY"))).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"DISTANCE")).limit(10);
		
		for(Document doc: sToFDestIt) {
			System.out.println(doc.toJson());
		}		
    }
    
    /*
     * Function Three:
     * 
     * Retrieves all flight counts in descending order received by Chicago, IL in each month
     */
    public static void numberOfFlightsPerAirport(MongoCollection<Document> collection) {
    		
    		AggregateIterable<Document> docs = collection.aggregate(
				Arrays.asList(
						Aggregates.match(Filters.eq("DEST_CITY_NAME","Chicago, IL")),
						Aggregates.group(new Document("$ifNull",Arrays.asList("$MONTH",false)), Accumulators.sum("count",1)),
						Aggregates.sort(Sorts.descending("count"))
						));
		
    		for(Document doc:docs) {
    			System.out.println(doc.toJson());
    		}
    		
    }
    
    /*
     * Function Four:
     * 
     * Retrieves flight information of flights that had a scheduled earliest or latest departure time 
     * in San Jose, CA on 1/2/09
     */
    public static void earlyLateDeparture(MongoCollection<Document> collection) {
    		
    		AggregateIterable<Document> docs = collection.aggregate(
    				Arrays.asList(
    						Aggregates.group(new Document("date","$FL_DATE").append("origin", "$ORIGIN_CITY_NAME"), Accumulators.max("latest", "$CRS_DEP_TIME"), Accumulators.min("earliest", "$CRS_DEP_TIME")),
    						Aggregates.match(Filters.and(Filters.eq("_id.origin", "San Jose, CA"),Filters.eq("_id.date","1/2/09")))
    						));
    		
    		for(Document doc:docs) {
    			System.out.println(doc.toJson());
    		}
    }
    
    /*
     * Function Five:
     * 
     * Retrieves flight information of flights that had an actual
     * arrival time after 18:00 (6 pm) in a Detroit, MI on a 4/1/09
     */
    public static void arriveAfter6pm(MongoCollection<Document> collection) {
    	
		FindIterable<Document> arrivalIt = collection.find(and(eq("DEST_CITY_NAME", "Detroit, MI"),eq("FL_DATE","4/1/09"),gt("ARR_TIME",1600))).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"DISTANCE")).limit(10);
		
		for(Document doc: arrivalIt) {
			System.out.println(doc.toJson());
		}
		
    }

    /*
     * Function Six:
     * 
     * Retrieves flight information of flights that were cancelled in 2009
     */
    public static void cancelledDates(MongoCollection<Document> collection) {
		
		FindIterable<Document> cancelledIt = collection.find(eq("CANCELLED",1)).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"CANCELLED",
				"CANCELLATION_CODE",
				"DISTANCE")).limit(10);
		
		for(Document doc: cancelledIt) {
			System.out.println(doc.toJson());
		}
    }
    
    /*
     * Function Seven:
     * 
     * Retrieves flight information of flights that had air time shorter than 120 mins
     */
    public static void max2HRFlights(MongoCollection<Document> collection) {
		
		FindIterable<Document> maxFlightIt = collection.find(lt("AIR_TIME",120)).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"AIR_TIME",
				"DISTANCE")).limit(10);
		
		for(Document doc: maxFlightIt) {
			System.out.println(doc.toJson());
		}
    }
    
    /*
     * Function Eight:
     * 
     * Retrieves flight information of all flights that had more than one diverted airport landings
     */
    public static void manyStops(MongoCollection<Document> collection) {
		
		FindIterable<Document> airportLandIt = collection.find(gt("DIV_AIRPORT_LANDINGS",1)).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"DISTANCE",
				"DIV_AIRPORT_LANDINGS")).limit(10);
		
		for(Document doc: airportLandIt) {
			System.out.println(doc.toJson());
		}
    }
    
    /*
     * Function Nine:
     * 
     * Retrieves information of flights that had distance that were greater
     * than 200 miles
     */
    public static void miPlusTrip(MongoCollection<Document> collection) {
		
		FindIterable<Document> distIt = collection.find(gt("DISTANCE",200)).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"DISTANCE")).limit(10);
		
		for(Document doc: distIt) {
			System.out.println(doc.toJson());
		}
    }
    
    /* Function Ten:
     * 
     * Retrieves flight information of flights that had real departure time
     * which was after 1300 and 2000
     */
    public static void after1Before8(MongoCollection<Document> collection) {
		
		FindIterable<Document> realDepIt = collection.find(and(gte("DEP_TIME",1300),(lte("DEP_TIME",2000)))).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"ARR_TIME",
				"DISTANCE")).limit(10);
		
		for(Document doc: realDepIt) {
			System.out.println(doc.toJson());
		}
    }
    
    /*
     * Function Eleven:
     * 
     * Retrieves the most popular flight destinations in 2009, count the number of flights
     * that arrive at the city in a year, and returns the top ten popular flight destination cities
     */
    public static void mostPopularDestPerQuarter(MongoCollection<Document> collection) {

    		AggregateIterable<Document> docs = collection.aggregate(
				Arrays.asList(
						Aggregates.group(new Document("$ifNull",Arrays.asList("$DEST_CITY_NAME",false)), Accumulators.sum("count",1)),
						Aggregates.sort(Sorts.descending("count")),
						Aggregates.limit(10)
						));
		
    		for(Document doc:docs) {
    			System.out.println(doc.toJson());
    		}		
    }
    
    /*
     * Function Twelve:
     * 
     * Retrieves the most popular week days (from 1 to 7) in 2009 that have maximum flight schedules, 
     * count the number of flights that get scheduled on that week day in a year, 
     * and return the top three popular week days.
     */
    public static void minOffAndOn(MongoCollection<Document> collection) {
    		
    		AggregateIterable<Document> docs = collection.aggregate(
				Arrays.asList(
						Aggregates.group(new Document("$ifNull",Arrays.asList("$DAY_OF_WEEK",false)), Accumulators.sum("count",1)),
						Aggregates.sort(Sorts.descending("count")),
						Aggregates.limit(3)
						));
		
    		for(Document doc:docs) {
    			System.out.println(doc.toJson());
    		}
    }
    
    /*
     * Function Thirteen
     * 
     * Retrieves count number of flights per quarter
     */
    public static void flightsPerQuarter(MongoCollection<Document> collection) {
    		
    		AggregateIterable<Document> docs = collection.aggregate(
				Arrays.asList(
						Aggregates.group(new Document("$ifNull",Arrays.asList("$QUARTER",false)), Accumulators.sum("count",1))
						));
		
    		for(Document doc:docs) {
    			System.out.println(doc.toJson());
    		}
    }
    
    /*
     * Function Fourteen
     * 
     * Retrieves the top ten cities that had most arrival delays in 2009
     */
    public static void multDelays(MongoCollection<Document> collection) {
    		
    		AggregateIterable<Document> docs = collection.aggregate(
				Arrays.asList(
						Aggregates.match(Filters.gt("ARR_DELAY_NEW", 0)),
						Aggregates.group(new Document("$ifNull",Arrays.asList("$DEST_CITY_NAME",false)), Accumulators.sum("count",1)),
						Aggregates.sort(Sorts.descending("count")),
						Aggregates.limit(10)
						));
		
    		for(Document doc:docs) {
    			System.out.println(doc.toJson());
    		}
    }    
    
    /*
     * Function Fifteen
     * 
     * Retrieves flights that arrival delay was shorter than the departure delay in 2009
     */
    public static void arrDellessdepDel(MongoCollection<Document> collection) {
    		
    		FindIterable<Document> realDepIt = collection.find(where("function() {return this. ARR_DELAY_NEW < this.DEP_DELAY_NEW}")).projection(Projections.include(
				"FL_DATE",
				"OP_UNIQUE_CARRIER",
				"TAIL_NUM",
				"OP_CARRIER_FL_NUM",
				"ORIGIN_CITY_NAME",
				"DEST_CITY_NAME",
				"DEP_TIME",
				"DEP_DELAY_NEW",
				"ARR_TIME",
				"ARR_DELAY_NEW",
				"DISTANCE")).limit(10);
		
		for(Document doc: realDepIt) {
			System.out.println(doc.toJson());
		}
    }
}