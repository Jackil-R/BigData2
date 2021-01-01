import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.CORBA.Context;
import org.apache.hadoop.fs.*;
import org.apache.commons.lang.StringUtils;
import java.util.*;


public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	String dump = value.toString();
	
    	if(StringUtils.ordinalIndexOf(dump,":",4)>-1){
		String[] values = dump.split("::");
		String rating = values[2];
		String movieID=values[1];
		Double a = Double.parseDouble(rating);	
			if(a == 5){
				data.set(movieID);
				context.write(data,one);}
				
		
	}
    }
 }




