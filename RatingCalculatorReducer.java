
/* RatingCalculatorReducer.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;










// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.util.regex.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.*;


public class RatingCalculatorReducer extends Reducer<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountReducer");
	  class Rating {
		 public String identifier;
		 public List<List<Number>>  topicratings = new ArrayList<List<Number>>();
		 public List<List<String>>  topictopics = new ArrayList<List<String>>();
		
	 }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException{
    	List<Rating> ratings = new ArrayList<Rating>() ;
    	for(Text value : values) {
    		String BusinessRecord = value.toString();
    		//String Review = value.toString();
    		if( BusinessRecord.length()==0 ) {
    			return;
    		}
    		ObjectMapper mapper = new ObjectMapper();
    		BusinessLdaFormat bdata = mapper.readValue(BusinessRecord, BusinessLdaFormat.class);
    		String type = bdata.getType();
    		Rating rateObj = null;
    		Boolean found = false;
    		for(Rating r: ratings) {
    			if( r.identifier.equals(type)) {
    				found = true;
    				rateObj = r;
    			}
    		} 
    		if(!found) {
    			rateObj = new Rating();
    			rateObj.identifier = type;
    			for(int i =0 ; i<8 ; i++) {
    				rateObj.topicratings.add(new ArrayList<Number>());
    			}
    			ratings.add(rateObj);
    		
    		//read topic data for current identifier
    		String filename = "./test2_data/"+type+"_data/model-final.twords";
    		File f = new File(filename);
    		if(!f.exists()) {
    			return;
    		}
    		BufferedReader in
    		   = new BufferedReader(new FileReader(filename));
    		String line;
    		Boolean newTopic = false;
    		List topic = null; 
    		// read file and push topics
    		while((line = in.readLine()) != null) {
    			String[] words = line.split("\\s+");
    			if(words[0].equals("Topic!")) {
    				if(topic != null) {
    					rateObj.topictopics.add(topic);
    				}
    				topic = new ArrayList<String>();
    			} else {
    				topic.add(words[0]);
    			}
    		}
    		}
    		
    		//check review text and add the rating to topic
    		String text = bdata.getText();
    		Number user_rating  = bdata.getReview_rating();
    		
    		String[] words = text.split("\\s+");
    		for(String word: words) {
    			int i =0;
    			for(List<String> str: rateObj.topictopics) { //topics
    				for(String t : str) {
    					if(word.startsWith(t)) {
    						//add the rating to the ratings
    						rateObj.topicratings.get(i).add(user_rating);
    						break;
    					}
    				}
    				i++;
    			}
    		}
    	} //end of for
    	for(Rating r: ratings) {
    		System.out.println(r.identifier + " with business_id"+ key);
    		int i = 0;
    		for(List<Number> stars : r.topicratings) {
    			if(stars.size() == 0) {
    				i++;
    				continue;
    			}
    			System.out.println("Topic: " + i);
    			float  sum = 0;
    			for(int j= 0 ; j< stars.size(); j++ ) {
    				sum = sum + stars.get(j).intValue();
    			}
    			float star = sum/stars.size();
    			//System.out.println("Number of stars: "+ star);
    			context.write(new Text(key +" " + r.identifier), new Text("Topic "+i +": " + star));
    			i++;
    		}
    	}
    	
    }
}