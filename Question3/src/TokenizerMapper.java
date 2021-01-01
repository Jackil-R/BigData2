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
    private String dump;
    private int startIndex;
    private int minimum;
    private int maximum;
    private String userInput="500";
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	ArrayList<String> top5 = new ArrayList<String> ();
	ArrayList<String> movieList = new ArrayList<String> ();
	BufferedReader in = new BufferedReader(new FileReader("part-r-00001"));
	       	
	String line="";
		
	for(int i=0;i<5;i++){
		
		if((line = in.readLine()) != null){
			String[] arr = line.split("	");
			top5.add(arr[0]);

		}
	}
	in.close();
	

	
	BufferedReader input = new BufferedReader(new FileReader("part-r-00000"));
	       	
	String line2="";
		
	while((line2 = input.readLine()) != null){
		String[] arr = line2.split("	");
		movieList.add(arr[0]);
	}
	input.close();


	dump = value.toString();
	
    	if(StringUtils.ordinalIndexOf(dump,":",4)>-1){
		String[] values = dump.split("::");
        	String movieID =values[0];
		String genre = values[2];

		if(!movieList.contains(movieID) && top5.contains(genre)){
			data.set(movieID);
			context.write(data,one);
				
		}
	}
	



    }
 }




