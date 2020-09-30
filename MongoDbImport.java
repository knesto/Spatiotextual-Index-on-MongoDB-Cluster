package gr.unipi.geotextualindexing.sfc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import com.mongodb.client.MongoCollection;

 public class MongoDbImport {
	
	protected MongoCollection <Document> coll;
	protected String filepath;
	
	protected MongoDbImport(MongoCollection <Document> coll,String filepath) {
        this.coll = coll;
        this.filepath=filepath;  

    }
    protected static Logger log = Logger.getLogger(MongoDbImport.class);
	protected static long[] MAX_RANGES;
    // Adding the multiple documents into the mongo collection.
    protected  void addMultipleDocuments() {
    	
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "\\|";
        int counter=1;
        //List<Document> doclist= new ArrayList<Document>();

        //set parameters for hilbert function
        int bits = 5;
        int dimensions = 2;
        //creation of hilbert object
        SmallHilbertCurve h = HilbertCurve.small().bits(bits).dimensions(dimensions);
        long maxOrdinates = 1L << bits;
        //Adding documents to collection
        try {
    
            br = new BufferedReader(new FileReader(this.filepath));
            while ((line = br.readLine()) != null) {
                
            	String[] data = line.split(cvsSplitBy);
            	String[] textdata = data[3].split(",");
                ArrayList< Double > array = new ArrayList< Double >();
                array.add( Double.valueOf(data[1]) );
                array.add( Double.valueOf(data[2]) );

                Document doc = new Document();
                doc.put("type","Point");
                doc.put("coordinates",array);
                
                //creation of documents per keyword 
                for (int i = 0; i <textdata.length; i++) {

	                Document finaldoc = new Document();
	                // creation a flag (groupid) for each document
	                finaldoc.put("groupid",counter);
	                finaldoc.put("Text",textdata[i].trim());
	                //creation of hilnert values for x,y and concatenation with keyword
	                finaldoc.put("hilbertindex",(String.valueOf(h.index(GeoUtil.scale2DPoint(Double.valueOf(data[1]), -180d,180d,Double.valueOf(data[2]), -90d,90d, maxOrdinates)))+textdata[i].trim()));
	                //finaldoc.put("1d",h.index(GeoUtil.scale2DPoint(Double.valueOf(data[4]), -180d,180d,Double.valueOf(data[3]), -90d,90d, maxOrdinates)));

	                finaldoc.put("location", doc);
	                this.coll.insertOne(finaldoc);
	                //doclist.add(finaldoc);
	              
                }
                counter++;
                
            }
            log.info("\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
       
        //this.coll.insertMany(doclist);
          

    }
 
	

}
