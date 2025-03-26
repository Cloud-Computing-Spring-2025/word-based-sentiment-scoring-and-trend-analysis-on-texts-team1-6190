package com.project.task2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TermCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text keyword, Iterable<IntWritable> occurrences, Context context)
         throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable frequency : occurrences) {
            total += frequency.get();
        }
        context.write(keyword, new IntWritable(total));
    }
}