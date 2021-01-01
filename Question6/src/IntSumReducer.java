

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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


public class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context)

              throws IOException, InterruptedException {

	ArrayList<String> top5 = new ArrayList<String> ();
	BufferedReader in = new BufferedReader(new FileReader("part-r-00001")); 
	String line="";	
	for(int i=0;i<5;i++)
	{
		
		if((line = in.readLine()) != null)
		{
			String[] arr = line.split("	");
			top5.add(arr[0]);
		}
	}
	in.close();

	String text =key.toString();
	//String genre = text.substring(StringUtils.ordinalIndexOf(text,"-",2)+1,text.length());

	
	
	int sum1 = 0;
	int sum2 = 0;
	int sum3 = 0;
	int sum4 = 0;
	int sum5 = 0;
	
	if(top5.get(0).contains(text)){

		for (Text value : values) {
			if(sum1<3){
				context.write(key, new Text(value));
			}
			sum1 = sum1+1;
		}
		
	
	}else if(top5.get(1).contains(text)){

		for (Text value : values) {
			if(sum2<3){
				context.write(key, new Text(value));
			}
			sum2 = sum2+1;
		}

	}else if(top5.get(2).contains(text)){

		for (Text value : values) {
			if(sum3<3){
				context.write(key, new Text(value));
			}
			sum3 = sum3+1;
		}
	
	}else if(top5.get(3).contains(text)){

		for (Text value : values) {
			if(sum4<3){
				context.write(key, new Text(value));
			}
			sum4 = sum4+1;
		}
	
	}else if(top5.get(4).contains(text)){


		for (Text value : values) {
			if(sum5<3){
				context.write(key, new Text(value));
			}
			sum5 = sum5+1;
		}
	}else{


	
	}



    }

}



