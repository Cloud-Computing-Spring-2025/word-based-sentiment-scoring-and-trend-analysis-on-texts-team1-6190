package com.project.task1;

import com.project.task1.BookMapper;
import com.project.task1.BookReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BookDataCleaner {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BookDataCleaner <input path> <output path>");
            System.exit(-1);
        }
        Configuration hadoopConfig = new Configuration();
        Job dataProcessingJob = Job.getInstance(hadoopConfig, "Book Data Processing Job");
        dataProcessingJob.setJarByClass(BookDataCleaner.class);
        dataProcessingJob.setMapperClass(BookMapper.class);
        dataProcessingJob.setReducerClass(BookReducer.class);
        dataProcessingJob.setOutputKeyClass(Text.class);
        dataProcessingJob.setOutputValueClass(Text.class);
        // Optionally set number of reducers (here, 1 to combine all records).
        dataProcessingJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(dataProcessingJob, new Path(args[0]));  // Folder with your text files.
        FileOutputFormat.setOutputPath(dataProcessingJob, new Path(args[1])); // Output folder.
        System.exit(dataProcessingJob.waitForCompletion(true) ? 0 : 1);
    }
}