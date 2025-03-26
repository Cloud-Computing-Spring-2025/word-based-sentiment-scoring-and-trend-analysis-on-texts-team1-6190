package com.project.task4;

import com.project.task4.BookYearMapper;
import com.project.task4.DecadeSumReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DecadeProcessor {
    public static void main(String[] cmdArgs) throws Exception {
        if (cmdArgs.length != 2) {
            System.err.println("Usage: DecadeProcessor <input directory> <output directory>");
            System.exit(-1);
        }

        Configuration hadoopConfig = new Configuration();
        Job processingJob = Job.getInstance(hadoopConfig, "Decade Data Aggregation");
        processingJob.setJarByClass(DecadeProcessor.class);

        processingJob.setMapperClass(BookYearMapper.class);
        processingJob.setReducerClass(DecadeSumReducer.class);
        processingJob.setCombinerClass(DecadeSumReducer.class); // Optional for optimization

        processingJob.setOutputKeyClass(Text.class);
        processingJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(processingJob, new Path(cmdArgs[0]));
        FileOutputFormat.setOutputPath(processingJob, new Path(cmdArgs[1]));

        System.exit(processingJob.waitForCompletion(true) ? 0 : 1);
    }
}