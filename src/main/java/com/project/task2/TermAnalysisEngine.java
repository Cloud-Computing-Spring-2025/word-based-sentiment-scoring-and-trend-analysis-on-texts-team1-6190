package com.project.task2;

import com.project.task2.TermExtractionMapper;
import com.project.task2.TermCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TermAnalysisEngine {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TermAnalysisEngine <input path> <output path>");
            System.exit(-1);
        }

        Configuration hadoopConfig = new Configuration();
        Job analysisJob = Job.getInstance(hadoopConfig, "Text Term Analysis with Lemmatization");

        analysisJob.setJarByClass(TermAnalysisEngine.class);
        analysisJob.setMapperClass(TermExtractionMapper.class);
        analysisJob.setCombinerClass(TermCountReducer.class);
        analysisJob.setReducerClass(TermCountReducer.class);
        analysisJob.setOutputKeyClass(Text.class);
        analysisJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(analysisJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(analysisJob, new Path(args[1]));

        System.exit(analysisJob.waitForCompletion(true) ? 0 : 1);
    }
}