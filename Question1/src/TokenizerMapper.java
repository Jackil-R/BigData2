

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.CORBA.Context;
import org.apache.hadoop.fs.*;
import org.apache.commons.lang.StringUtils;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    private String dump;
    private int startIndex;
    private int minimum;
    private int maximum;
    private String userInput="500";
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
	dump = value.toString();
	
    	if(StringUtils.ordinalIndexOf(dump,":",6)>-1){
        	startIndex = StringUtils.ordinalIndexOf(dump,":",1);
        	String userID = dump.substring(0,startIndex);

		if(userID.equals(userInput))
		{
                   	int startI = StringUtils.ordinalIndexOf(dump,":",2)+1;
			int endI= StringUtils.ordinalIndexOf(dump,":",3);
			String movieID =  dump.substring(startI,endI);
			data.set(movieID);
			context.write(data,one); 
		}
	
      		

	}
    }
 }




