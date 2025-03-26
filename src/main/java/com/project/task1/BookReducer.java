package com.project.task1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BookReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // For our job, there should be one record per file.
        // If multiple records exist, concatenate them.
        StringBuilder combinedText = new StringBuilder();
        for (Text textChunk : values) {
            combinedText.append(textChunk.toString()).append(" ");
        }
        context.write(key, new Text(combinedText.toString().trim()));
    }
}