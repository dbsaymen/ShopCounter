package tn.enis.shop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>  {
	private FloatWritable value = new FloatWritable(0);
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
    	double sum = 0;
        for (FloatWritable value : values)
            sum =sum+  value.get();
        value.set((float)sum);
        context.write(key, value);
    }

}
