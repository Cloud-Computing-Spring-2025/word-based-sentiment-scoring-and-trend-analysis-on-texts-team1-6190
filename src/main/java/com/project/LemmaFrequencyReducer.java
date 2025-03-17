package com.project;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.project.LemmaFrequencyMapper.LemmaKey;

/**
 * Reducer for word frequency analysis with lemmatization.
 * 
 * This reducer:
 * 1. Aggregates the counts for each lemma per book and year
 * 2. Produces a dataset with each lemma and its frequency along with the book ID and year
 * 
 * Input: LemmaKey(bookId, lemma, year) -> Iterable<IntWritable>(1, 1, 1, ...)
 * Output: Text(bookId, lemma, year) -> IntWritable(frequency)
 */
public class LemmaFrequencyReducer extends Reducer<LemmaKey, IntWritable, Text, IntWritable> {
    
    @Override
    public void reduce(LemmaKey key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // Sum up the counts for this lemma
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        // Format the output key as (bookId,lemma,year)
        String outputKey = "(" + key.getBookId() + "," + key.getLemma() + "," + key.getYear() + ")";
        
        // Output the result with the lemma frequency
        context.write(new Text(outputKey), new IntWritable(sum));
    }
}