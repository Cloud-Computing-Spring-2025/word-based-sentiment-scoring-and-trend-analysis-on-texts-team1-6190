package com.project.task3;

import com.project.task3.SentimentMapper;
import com.project.task3.SentimentReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class SentimentAnalysisDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SentimentAnalysisDriver <input path> <output path> <AFINN file path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis");

        job.setJarByClass(SentimentAnalysisDriver.class);
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));  // Input: Task 2 Output
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output: Sentiment Score per Volume-Year

        // Distributed Cache for AFINN file (in input folder)
        job.addCacheFile(new URI(args[2] + "#AFINN-111.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}