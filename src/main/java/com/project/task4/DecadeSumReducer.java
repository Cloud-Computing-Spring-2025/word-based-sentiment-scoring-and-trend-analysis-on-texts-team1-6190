package com.project.task4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DecadeSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text compositeKey, Iterable<IntWritable> countValues, Context context)
            throws IOException, InterruptedException {
        int aggregatedSum = 0;
        for (IntWritable currentValue : countValues) {
            aggregatedSum += currentValue.get();
        }
        context.write(compositeKey, new IntWritable(aggregatedSum));
    }
}