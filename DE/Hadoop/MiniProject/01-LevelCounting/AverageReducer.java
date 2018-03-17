package com.ybigta.hadoopproject201803;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int num = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            num++;
        }
        result.set((double)sum/num);
        context.write(key, result);
    }
}
