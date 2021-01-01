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

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    private String dump;
    private int startIndex;
    private int minimum;
    private int maximum;
    private String userInput="500";
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	ArrayList<String> movieList = new ArrayList<String>();
		
	BufferedReader in = new BufferedReader(new FileReader("part-r-00000"));
	       	
	String line="";
		
	while((line = in.readLine()) != null){
		String[] arr = line.split("	");
		movieList.add(arr[0]);
	}

	in.close();

	dump = value.toString();
	
    	if(StringUtils.ordinalIndexOf(dump,":",4)>-1){
		String[] values = dump.split("::");
        	String movieID =values[0];

		if(movieList.contains(movieID)){
			

			String[] genre= values[2].split(Pattern.quote("|"));
			for(int i=0; i<genre.length;i++){
				data.set(genre[i]);
				context.write(data,one);
			}	
		}
	}
	
    }
 }




