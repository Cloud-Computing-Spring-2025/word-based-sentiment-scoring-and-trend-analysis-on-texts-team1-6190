package com.project.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.project.LemmaFrequencyMapper;
import com.project.LemmaFrequencyReducer;
import com.project.LemmaFrequencyMapper.LemmaKey;

/**
 * Driver class for the Word Frequency Analysis with Lemmatization MapReduce job.
 * 
 * This class configures and runs a MapReduce job that:
 * 1. Takes the cleaned dataset from Task 1
 * 2. Splits text into words and applies Stanford CoreNLP lemmatization
 * 3. Computes the frequency of each lemma per book and year
 * 4. Produces a dataset listing each lemma with its frequency and metadata
 * 
 * Usage: hadoop jar <jar-file> WordFrequencyAnalyzer <input-path> <output-path>
 */
public class WordFrequencyAnalyzer extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordFrequencyAnalyzer <input path> <output path>");
            return -1;
        }

        // Create and configure a new job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Word Frequency Analysis with Lemmatization");
        
        // Set the driver class
        job.setJarByClass(WordFrequencyAnalyzer.class);
        
        // Set mapper and reducer classes
        job.setMapperClass(LemmaFrequencyMapper.class);
        job.setReducerClass(LemmaFrequencyReducer.class);
        
        // Set map output key and value classes
        job.setMapOutputKeyClass(LemmaKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
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
        int exitCode = ToolRunner.run(new WordFrequencyAnalyzer(), args);
        System.exit(exitCode);
    }
}