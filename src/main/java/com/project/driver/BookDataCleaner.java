package com.project.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.project.BookMapper;
import com.project.BookReducer;

/**
 * Driver class for the Book Preprocessing MapReduce job.
 * 
 * This class configures and runs a MapReduce job that:
 * 1. Cleans and standardizes raw text data from book files
 * 2. Extracts essential metadata (book ID, publication year)
 * 3. Produces a preprocessed dataset in the format: (bookId,year) cleaned_text
 * 
 * Usage: hadoop jar <jar-file> BookDataCleaner <input-path> <output-path>
 */
public class BookDataCleaner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BookDataCleaner <input path> <output path>");
            return -1;
        }

        // Create and configure a new job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Book Data Preprocessing");
        
        // Set the driver class
        job.setJarByClass(BookDataCleaner.class);
        
        // Set mapper and reducer classes
        job.setMapperClass(BookMapper.class);
        job.setReducerClass(BookReducer.class);
        
        // Set map output key and value classes
        job.setMapOutputKeyClass(BookMapper.BookMetadataKey.class);
        job.setMapOutputValueClass(Text.class);
        
        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Check if output directory exists and delete it if it does
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output directory " + outputPath + " deleted.");
        }
        
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // Submit the job and wait for completion
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BookDataCleaner(), args);
        System.exit(exitCode);
    }
}