package gr.unipi.geotextualindexing.sfc;

import java.text.ParseException;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;


public class BoxRangeQueries  {
	
	protected MongoCollection <Document> coll;
	
	protected BoxRangeQueries (MongoCollection <Document> coll) {
        this.coll = coll;      
    }
	
    protected  void executeQuery(MongoCollection<Document> mongoCollection, List<Bson> list){
        long t1 = System.currentTimeMillis();
        int count=0;
        MongoCursor<Document> cursor = mongoCollection.aggregate(list).iterator();
        //System.out.println(list);
        try {
            while (cursor.hasNext()) {
            	cursor.next().toJson();
                //System.out.println(cursor.next().toJson());
                count++;
                //if (count>101) {cursor.close();break;}
            }
        } finally {
            cursor.close();
        }
        System.out.println(count);
        System.out.println("Calculation Time: " + (System.currentTimeMillis() - t1));
    }
    
    protected  String getJsonQuery(double lon1,double lon2, double minLon, double maxLon, double lat1,double lat2, double minLat, double maxLat, String keywords[],long max,SmallHilbertCurve hc)throws ParseException {
        Ranges rangesList = hc.query(GeoUtil.scale2DPoint(lon1, minLon,maxLon,lat1, minLat,maxLat, max),
        		GeoUtil.scale2DPoint(lon2, minLon,maxLon,lat2, minLat,maxLat, max));
        StringBuilder sb = new StringBuilder();
        StringBuilder sbkewords = new StringBuilder();
        //System.out.print(" ranges found: "+rangesList.size()  + " ");
        //retrieve hilbert values and concatenation of keywords 
      for(int word =0;word<keywords.length;word++){
    	  String keyword=keywords[word];
    	  sbkewords.append("\""+keyword+"\""+",");
	        rangesList.stream().forEach(i->{
	       
	                for(long k=i.low(); k<= i.high(); k++){
	                    sb.append("\""+k+keyword+"\""+",");
	                    
	                }
	            
	        });
      }   
        
        sb.deleteCharAt(sb.length()-1);
        //sbkewords.deleteCharAt(sbkewords.length()-1);
        
        //return "{$match:{hilbertindex:{$in:["+sb.toString()+"]}}}";
        //return "{$match:{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}} }}";
        //return "{$match:{Text:{$in:["+sbkewords.toString()+" ] }}}";
        //return "{$match:{ location: { $geoWithin: { $geometry: { type : \"Polygon\" , coordinates: [ [ [ 23.757495, 37.987295 ], [23.766958, 37.987295], [ 10.2, 32.3 ],  [23.757495, 37.987295] ] ] } } } }}";

       // return "{ $match: { $and: [  { location: { $geoWithin: { $geometry: { type : \"Polygon\" , coordinates: [ [ [ "+lon1+", "+lat1+" ], ["+lon2+", "+lat2+"], [ "+lon1+", "+lat1+"] ] ] } } } },{Text:{$in:[ "+sbkewords.toString()+"] }},{hilbertindex:{$in:["+sb.toString()+"]}}]}}";        
        
        //return "{$match:{ $and: [{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}}},{Text:{$in:[ "+sbkewords.toString()+"] }}]}}";
        //return "{$match:{ $and: [{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}}},{$text:{$search:\"Cambodian Lebanese Persian\" }}]}}";
        
        return "{$match:{ $and: [{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}}},{Text:{$in:[ "+sbkewords.toString()+"] }},{hilbertindex:{$in:["+sb.toString()+"]}}]}}";

        
  		
    }
    //get number of documents
    protected  String getDoucumentsCount(){
        return "{ $group: { _id: null , count: {$sum: 1} } }";
    }
    protected  String getDoucumentsGroupidfirst(){
        return "{ $group: { _id: \"$groupid\", temp: {$first: 1} } }";
    }
    // get number of groupids
    protected  String getDoucumentsCountWithGroupid(){
        return "{ $group: { _id: \"$_id.groupid\", count: {$sum: \"$temp\"} } }";  
    }

}
