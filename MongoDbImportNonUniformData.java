package gr.unipi.geotextualindexing.sfc;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDbImportNonUniformData {
	
	 @Test
	 public void main() throws Exception {
		 
		 // Mongodb initialization parameters.
	        int port_no = 27017;
	        String host_name = "localhost", db_name = "mongodb", db_coll_name = "non_uniform_collection";
	 
	        // Mongodb connection string.
	        String client_url = "mongodb://" + host_name + ":" + port_no + "/" + db_name;
	        MongoClientURI uri = new MongoClientURI(client_url);
	 
	        // Connecting to the mongodb server using the given client uri.
	        MongoClient mongo_client = new MongoClient(uri);
	 
	        // Fetching the database from the mongodb.
	        MongoDatabase db = mongo_client.getDatabase(db_name);
	 
	        // Fetching the collection from the mongodb.
	        MongoCollection<Document> coll = db.getCollection(db_coll_name);
	 
	        String filepath = "/home/user/mongo/Non_UniformDataset/non_uniform_dataset.txt";
	        
	        MongoDbImport md= new MongoDbImport(coll,filepath);
	        md.addMultipleDocuments();
	        mongo_client.close();
		 
	 }
	 
	    public static void main(String[] args) throws Exception {

	    	org.junit.runner.JUnitCore.main("gr.unipi.geotextualindexing.sfc.MongoDbImportNonUniformData");            
	 }

}
