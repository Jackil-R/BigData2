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


public class TokenizerMapper extends Mapper<Object, Text, Text, Text> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	ArrayList<String> unwatched = new ArrayList<String>();
	ArrayList<Integer> numOfRatings = new ArrayList<Integer>();
	ArrayList<String> top5 = new ArrayList<String> ();	
	
	




	BufferedReader input = new BufferedReader(new FileReader("part-r-00000"));
	String line2="";
	int sum=0;	
	while((line2 = input.readLine()) != null){
		String[] arr = line2.split("	");
		unwatched.add(arr[0]);
		numOfRatings.add(Integer.parseInt(arr[1]));
		sum=sum+Integer.parseInt(arr[1]);
	}

	input.close();
		
	int avg=sum/numOfRatings.size();


	String dump = value.toString();
	if(StringUtils.ordinalIndexOf(dump,":",4)>-1){
		String[] values = dump.split("::");
		String ID = values[0];
		String title = values[1];
		String[] genre= values[2].split(Pattern.quote("|"));
			
					
		 

		if(unwatched.contains(ID)){  // checks with all unwatched movies	
			data.set(values[2]);
			context.write(data,new Text(ID+"   -   "+title));
		}
	}

    }
}
