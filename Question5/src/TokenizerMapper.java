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
    //private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	ArrayList<String> fiveStarID = new ArrayList<String>();
	ArrayList<Integer> numOfRatings = new ArrayList<Integer>();		
	
	BufferedReader in = new BufferedReader(new FileReader("part-r-00000"));
	       	
	String line="";
		
	while((line = in.readLine()) != null){
		String[] arr = line.split("	");
		fiveStarID.add(arr[0]);
		numOfRatings.add(Integer.parseInt(arr[1]));
	}

	
	in.close();


	String dump = value.toString();
	
		String[] values = dump.split("	");
		String unwatchedID = values[0];
		
		if(fiveStarID.contains(unwatchedID))
		{
					data.set(unwatchedID);
					IntWritable one = new IntWritable(numOfRatings.get(fiveStarID.indexOf(unwatchedID)));
					context.write(data, one);
		}
	


    }
}




