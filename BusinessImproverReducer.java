
/* BusinessImproverReducer.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */
/* this is phase 1 reducer*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;




// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.*;

/**
 *
 * @author mac
 */
public class BusinessImproverReducer extends Reducer<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountReducer");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
    	
    	String BusinessRecord = key.toString();
    	//String Review = value.toString();
    	if( BusinessRecord.length()==0 ) {
    		return;
    	}
    	Boolean hasReview = false, hasType = false;
    	List<String> catag = new ArrayList<String>();
    	ObjectMapper mapper = new ObjectMapper();
    	String search = "\"type\": \"review\"";
    	String search2 = "\"type\": \"business\"";
    	BusinessCombined format = new BusinessCombined();
    	for(Text val : values) {
    		String Businesstr = val.toString();
    		if(Businesstr.indexOf(search2) != -1 ) {
    			BusinessAttrib bdata = mapper.readValue(Businesstr, BusinessAttrib.class);
    			for(Object busCat: bdata.getCategories()) {
    				catag.add(busCat.toString());
    			}
    			format.setBusiness_id(BusinessRecord);
    			format.setBusiness_rating(bdata.getStars());
    			
    			hasType = true;
    			break;
    		}
    	}
    	
    	for(Text val : values) {
    		String Businesstr = val.toString();
    		format.setBusiness_id(BusinessRecord);
    		if(Businesstr.indexOf(search) != -1 ) {
    			BusinessFormater bdata = mapper.readValue(Businesstr, BusinessFormater.class);
    			//context.write(new Text(bdata.getBusiness_id()), new Text(new Text(BusinessRecord)));
    			format.setText(bdata.getText());
    			format.setReview_rating(bdata.getStars());
    			hasReview = true;
    			if(hasType) {
    				for(String type: catag) {
    				format.setType(type);
    				String output = mapper.writeValueAsString(format);
    				context.write(new Text(type), new Text(output));
    				}
    			}
    		}
    	}
    	
    	/*String txt = null;
    	for(Text val : values) {
    		txt += "\t"+val;
    	}
    	context.write(key, new Text(txt));*/
    }
}