package com.project;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for the Book Preprocessing MapReduce job.
 * 
 * This reducer:
 * 1. Receives processed text along with metadata from the Mapper
 * 2. Produces a cleaned dataset with the essential metadata and preprocessed text
 * 
 * Input: BookMetadataKey(bookId, year) -> Iterable<Text> containing title and preprocessed text
 * Output: Key: (bookId,year), Value: cleaned text
 */
public class BookReducer extends Reducer<BookMapper.BookMetadataKey, Text, Text, Text> {
    
    @Override
    public void reduce(BookMapper.BookMetadataKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        StringBuilder cleanedText = new StringBuilder();
        String title = "";
        
        for (Text value : values) {
            // Extract title and cleaned text from the value
            String[] parts = value.toString().split("\t", 2);
            
            if (title.isEmpty()) {
                title = parts[0];  // Store the title for reference
            }
            
            // Append the cleaned text part
            String textPart = parts.length > 1 ? parts[1] : "";
            if (!textPart.isEmpty()) {
                cleanedText.append(textPart).append(" ");
            }
        }
        
        // Format the output key as (bookId,year)
        String outputKey = "(" + key.getBookId() + "," + key.getYear() + ")";
        
        // Output the result with the requested format
        context.write(new Text(outputKey), new Text(cleanedText.toString().trim()));
    }
}