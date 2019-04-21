package tn.enis.shop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
	private Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] t=line.split("\t");
      
        word.set(t[2]);
        Float a=Float.parseFloat(t[4]);
        
        FloatWritable one = new FloatWritable(a);
        context.write(word,one);
    }
}
