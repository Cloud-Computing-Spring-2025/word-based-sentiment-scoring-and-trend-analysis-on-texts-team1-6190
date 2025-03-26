package com.project.task3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable resultValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable val : values) {
            total += val.get();
        }
        resultValue.set(total);
        context.write(key, resultValue);
    }
}