
/* RatingCalculatorMapper.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */
import java.io.IOException;
import java.util.List;
import java.lang.*;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

public class RatingCalculatorMapper extends Mapper<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.smaple.HomeworkMapper");

    @Override
    protected void map(Text key, Text value, Context context)
                    throws IOException, InterruptedException {
    
         String BusinessRecord = value.toString();
    	//String Review = value.toString();
    	if( BusinessRecord.length()==0 ) {
    		return;
    	}
    	
    	ObjectMapper mapper = new ObjectMapper();
    	
    	BusinessLdaFormat bdata = mapper.readValue(BusinessRecord, BusinessLdaFormat.class);
    	
        context.write(new Text(bdata.getBusiness_id()),value);
    }
}