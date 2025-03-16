package com.project;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for the Book Preprocessing MapReduce job.
 * 
 * Input: Each line from the raw text or CSV file
 * Output: BookMetadataKey(bookId, year) -> Text(title + cleaned text)
 */
public class BookMapper extends Mapper<Object, Text, BookMapper.BookMetadataKey, Text> {
    
    /**
     * Custom composite key class for the Book Preprocessing MapReduce job.
     */
    public static class BookMetadataKey implements WritableComparable<BookMetadataKey> {
        private String bookId;
        private int year;

        // Default constructor required for Hadoop serialization
        public BookMetadataKey() {
        }

        // Constructor with fields
        public BookMetadataKey(String bookId, int year) {
            this.bookId = bookId;
            this.year = year;
        }

        // Setter method
        public void set(String bookId, int year) {
            this.bookId = bookId;
            this.year = year;
        }

        // Write object state to output stream
        @Override
        public void write(java.io.DataOutput out) throws IOException {
            WritableUtils.writeString(out, bookId);
            out.writeInt(year);
        }

        // Read object state from input stream
        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            this.bookId = WritableUtils.readString(in);
            this.year = in.readInt();
        }

        // Compare this object with another for sorting
        @Override
        public int compareTo(BookMetadataKey other) {
            int cmp = this.bookId.compareTo(other.bookId);
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(this.year, other.year);
        }

        // Check equality between this object and another
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof BookMetadataKey) {
                BookMetadataKey other = (BookMetadataKey) obj;
                return this.bookId.equals(other.bookId) && this.year == other.year;
            }
            return false;
        }

        // Generate hash code for this object
        @Override
        public int hashCode() {
            return bookId.hashCode() * 163 + year;
        }

        // String representation of this object
        @Override
        public String toString() {
            return bookId + "\t" + year;
        }

        // Getter methods
        public String getBookId() {
            return bookId;
        }

        public int getYear() {
            return year;
        }
    }
    
    private Set<String> stopWords = new HashSet<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize stop words list
        stopWords.addAll(Arrays.asList(
            "a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "by",
            "from", "in", "out", "with", "about", "as", "into", "like", "of", "till", "is",
            "am", "are", "was", "were", "be", "being", "been", "have", "has", "had", "do",
            "does", "did", "can", "could", "will", "would", "shall", "should", "may", "might",
            "must", "i", "you", "he", "she", "it", "we", "they", "this", "that", "these", 
            "those", "my", "your", "his", "her", "its", "our", "their"
        ));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Parse the input line (CSV format)
            String line = value.toString();
            String[] parts = line.split(",", 4);
            
            if (parts.length < 4) {
                // Skip malformed lines
                return;
            }
            
            String bookId = parts[0].trim();
            String title = parts[1].trim();
            int year;
            
            try {
                year = Integer.parseInt(parts[2].trim());
            } catch (NumberFormatException e) {
                // Skip records with invalid years
                return;
            }
            
            String text = parts[3];
            
            // Clean and preprocess the text
            String cleanedText = preprocessText(text, stopWords);
            
            // Create composite key (bookId, year)
            BookMetadataKey outputKey = new BookMetadataKey(bookId, year);
            
            // Output with title included in the value for reference
            Text outputValue = new Text(title + "\t" + cleanedText);
            
            context.write(outputKey, outputValue);
        } catch (Exception e) {
            // Log error and continue processing other records
            System.err.println("Error processing input: " + value);
            e.printStackTrace();
        }
    }
    
    /**
     * Preprocess text by converting to lowercase, removing punctuation, and filtering stop words
     */
    private String preprocessText(String text, Set<String> stopWords) {
        // Convert to lowercase
        text = text.toLowerCase();
        
        // Remove punctuation
        text = text.replaceAll("[^a-zA-Z0-9\\s]", " ");
        
        // Tokenize and remove stop words
        StringBuilder preprocessedText = new StringBuilder();
        StringTokenizer tokenizer = new StringTokenizer(text);
        
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (!stopWords.contains(token)) {
                preprocessedText.append(token).append(" ");
            }
        }
        
        return preprocessedText.toString().trim();
    }
}