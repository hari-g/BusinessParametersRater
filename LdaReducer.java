/* LdaReducer.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.File;
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

/**
 *
 * @author Hari Krishna Gajarla
 */
public class LdaReducer extends Reducer<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountReducer");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException{
    	String key_string  = key.toString();
    	/*if(key_string.compareTo("Restaurants") != 0) {
    		return;
    	} */
    	Pattern stopWords = Pattern.compile("\\b(?:i|a|and|about|an|are|for|was|you|"
    			+ "the|and|on|here|it|was|to|like|liked|of|much|very|can|food|that|much|"
    			+ "glad|yes|we|she|he|they|them|okay|good|bad|amaing|does|did|eat|actual|my|which|wife|husband|"
    			+ "ate|hit|u|b|c|d|how|nice|be|saturday|sunday|monday|tuesday|wednesday|thursday|firday|month|"
    			+ "thinking|must|crazy|am|would|very|prepare|&|in|out|left|right|write|yelp|be|mention|did|done|"
    			+ "bake|have|lots|lot|shit|bro|menu|think|or|all|ming|car|little|large|big|same|but|better|"
    			+ "ming|ing|thought|haven|t|was|their|is|oily|there|i'had|had|has|night|day|"
    			+ "at|as|two|one|bike|white|this|that|them|thus|were|me|do|black|grey|green|red|so|"
    			+ "many|few|most|if|it|icon|move|walk|run|late|pick|pickup|sure|surely|not|be|visit|visiting|"
    			+ "will|great|best|with|love|hate|will|s|am|pros|cons|what|also|place|always|really|"
    			+ "at|your|us|'ve|our|go|from|to|just|up|down|back|order|ordered|bar|some|when|no|been|restaurant|got|because|"
                + "her|by|minutes|seconds|minute|second|after|before|infront|back|didn|did|haven|not|n|by|"
                + "well|'m|'re|only|came|come|can't|can|could|shall|would|took|even|then|than|too|hot|more|don't|"
                + "time|mexican|chinese|try|tried|t|s|m|re|ve|1|2|3|4|5|6|7|8|9|10|11|12|13|14|20|selection|went|who|said|told|off|"
                + "another|never|experience|again|other|pittsburg|home|know|amazing|excellent|his|wasn't|"
                + "last|want|wanted|around|round|dark|take|taking|first|second|asked|over|people|italian|made|both|"
                + "definitely|dish|going|gone|still|way|long|tall|short|pretty|its|it|find|side|should|times|who|sauce|drink|bar|night|sad|"
                + "happy|stay|iam|impressed|sheer|pleasure|of|heavy|noice|level|bring|great|spill|let's|least|extreme|help|since|"
                + "with|multiple|believe|make|sure|your|up|i|ve|in|addition|subtraction|can|t|won|t|i|m|"
                + "any|do|bunch|run|into|beer|while|expect|review|star|now|ever|stop|though|drink|don|where|had|work|phoenix|oh|mean|year|old)\\b\\s*", Pattern.CASE_INSENSITIVE);
    	
    	String filename = key.toString()+"_Reviews.txt";
    	String dirname = "./demo_data/"+key.toString()+"_data";
    	String currfile = dirname+"/"+filename;
    	new File(dirname).mkdir();
    	PrintWriter bw = new PrintWriter(currfile, "UTF-8");
		
		int length = 0;
    	for(Text value: values) {
    		// write values to a file and perform cleanup
    		
    		String rec = value.toString();
    		// elimiate the reviews with no matter! e.g: It's good!!!
    		if(rec.length() < 40) {
    			continue;
    		}
    		rec = rec.replace("\n", " ");
    		rec = rec.replace('.', ' ');
    		rec = rec.replace('\"', ' ');
    		rec = rec.replace(',', ' ');
    		rec = rec.replace('!', ' ');
    		rec = rec.replace('\'', ' ');
    		rec = rec.replace('=', ' ');
    		rec = rec.replace('?', ' ');
    		rec = rec.replace(')', ' ');
    		rec = rec.replace('(', ' ');
    		rec = rec.replace('+', ' ');
    		rec = rec.replace('-', ' ');
    		rec = rec.replace('_', ' ');
    		rec = rec.replace('&', ' ');
    		rec = rec.replace(':', ' ');
    		rec = rec.replace(';', ' ');
    		
    		
    		Matcher matcher = stopWords.matcher(rec);
        	String clean = matcher.replaceAll("");
    		clean = clean.toLowerCase();
    		String words[]  = clean.split(" ");
    		String stemmed = "";
    		for(String word: words) {
    			Stemmer ps  = new Stemmer();
    			char[] chrarray = word.toCharArray();
    			for(char chr: chrarray) {
    				if(Character.isLetter(chr)) {
    					ps.add(chr);
    				}
    			}
    			ps.stem();
    			stemmed += " " + ps.toString();
    		}
    		
    		bw.println(stemmed);
    		length++;
    	}
    	bw.close();
    	
    	// append number of documents at top
    	RandomAccessFile prevfile = new RandomAccessFile(new File(currfile), "rw");
    	prevfile.seek(0); 
    	String header = Integer.toString(length)+"\n";
    	prevfile.write(header.getBytes());
    	prevfile.close();
    	
    	
    	LDACmdOption option = new LDACmdOption();
    	option.alpha = 50.0/6.0;
    	option.beta  = 0.1;
    	option.est = true;
    	option.dir = dirname;
    	option.dfile = filename;
    	//option.dfile = "xaj_sps5";
    	option.modelName = key.toString() + "_model";
    	option.K = 6;
    	option.niters = 100;
    	option.twords = 7;
    	option.wordMapFileName = key.toString() + "_wordmap.txt";
    	
		
		try {
			
			//parser.parseArgument(args);
			
			if (option.est || option.estc){
				Estimator estimator = new Estimator();
				estimator.init(option);
				estimator.estimate();
			}
			else if (option.inf){
				Inferencer inferencer = new Inferencer();
				inferencer.init(option);
				
				Model newModel = inferencer.inference();
			
				for (int i = 0; i < newModel.phi.length; ++i){
					//phi: K * V
					System.out.println("-----------------------\ntopic" + i  + " : ");
					for (int j = 0; j < 10; ++j){
						System.out.println(inferencer.globalDict.id2word.get(j) + "\t" + newModel.phi[i][j]);
					}
				}
			}
		}
		catch (Exception e){
			System.out.println("Error in main: " + e.getMessage());
			e.printStackTrace();
			return;
		}
    }
}