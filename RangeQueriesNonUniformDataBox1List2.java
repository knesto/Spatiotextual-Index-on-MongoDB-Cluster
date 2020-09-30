package gr.unipi.geotextualindexing.sfc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.bson.Document;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.junit.*;
import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



public class RangeQueriesNonUniformDataBox1List2 {
	
	 @Test
	 public void test1() throws Exception { 
		 // Mongodb initialization parameters.
	        int port_no = 27017;
	        String host_name = "localhost", db_name = "mongodb", db_coll_name = "non_uniform_collection";
	        //hilbertcollection
	        // Mongodb connection string.
	        String client_url = "mongodb://" + host_name + ":" + port_no + "/" + db_name;
	        MongoClientURI uri = new MongoClientURI(client_url);
	 
	        // Connecting to the mongodb server using the given client uri.
	        MongoClient mongo_client = new MongoClient(uri);
	 
	        // Fetching the database from the mongodb.
	        MongoDatabase db = mongo_client.getDatabase(db_name);

	        // Fetching the collection from the mongodb.
	        MongoCollection<Document> coll = db.getCollection(db_coll_name);
	        
	        int bits = 5;
	        int dimensions = 2;
	        SmallHilbertCurve h = HilbertCurve.small().bits(bits).dimensions(dimensions);
	        long max = 1L << bits;
	        double lon1= 31.02734375;
	        double lat1=  55.97531083569679;
	        double lon2= 30.7284375;
	        double lat2= 55.968945343432936;
	        double maxlon=180d;
	        double minlon=-180d;
	        double maxlat=90d;
	        double minlat=-90d;
	        String[] keywords = {"abdominal","abdicated","abdicator"};
	 
	       BoxRangeQueries rq= new BoxRangeQueries(coll);
	       //rq.executeQuery(coll, Arrays.asList(Document.parse(rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h))));
	       //rq.executeQuery(coll, Arrays.asList(Document.parse(rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)),Document.parse(rq.getDoucumentsCount())));    
	       //rq.executeQuery(coll, Arrays.asList(Document.parse(rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)),Document.parse(rq.getDoucumentsGroupidfirst()),Document.parse(rq.getDoucumentsCountWithGroupid())));    
		 
	       
	       //executionStats
	       Gson gson = new Gson();
	       //System.out.println(gson.toJson(db.runCommand(Document.parse("{ \"explain\": { \"aggregate\": \""+db_coll_name+"\", pipeline:["+rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)+"], \"cursor\":{} }}"))));
	       //System.out.println(gson.toJson(db.runCommand(Document.parse("{ \"explain\": { \"aggregate\": \""+db_coll_name+"\", pipeline:["+rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)+","+rq.getDoucumentsGroupidfirst()+","+rq.getDoucumentsCountWithGroupid()+"], \"cursor\":{} }}"))));
	   	   
	       String fileContent = gson.toJson(db.runCommand(Document.parse("{ \"explain\": { \"aggregate\": \""+db_coll_name+"\", pipeline:["+rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)+"], \"cursor\":{} }}")));
	       FileOutputStream outputStream = new FileOutputStream("results_non_uniform_q2.txt");
	       byte[] strToBytes = fileContent.getBytes();
	       outputStream.write(strToBytes);
	     
	       outputStream.close();
	       
	        mongo_client.close();
	 }
	 
	    public static void main(String[] args) throws Exception {

	    	org.junit.runner.JUnitCore.main("gr.unipi.geotextualindexing.sfc.RangeQueriesNonUniformDataBox1List2");            
	 }

}
