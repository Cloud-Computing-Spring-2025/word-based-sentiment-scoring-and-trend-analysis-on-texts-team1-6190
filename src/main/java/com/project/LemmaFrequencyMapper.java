package com.project;

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

/**
 * Mapper for word frequency analysis with lemmatization.
 * 
 * Input: The cleaned dataset from Task 1 in format (bookId,year) cleaned_text
 * Output: Key-value pairs with key as (bookID, lemma, year) and value as 1
 */
public class LemmaFrequencyMapper extends Mapper<Object, Text, LemmaFrequencyMapper.LemmaKey, IntWritable> {
    
    /**
     * Custom composite key class that includes bookId, lemma, and year
     */
    public static class LemmaKey implements WritableComparable<LemmaKey> {
        private String bookId;
        private String lemma;
        private int year;
        
        // Default constructor required for Hadoop serialization
        public LemmaKey() {
        }
        
        public LemmaKey(String bookId, String lemma, int year) {
            this.bookId = bookId;
            this.lemma = lemma;
            this.year = year;
        }
        
        @Override
        public void write(java.io.DataOutput out) throws IOException {
            WritableUtils.writeString(out, bookId);
            WritableUtils.writeString(out, lemma);
            out.writeInt(year);
        }
        
        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            bookId = WritableUtils.readString(in);
            lemma = WritableUtils.readString(in);
            year = in.readInt();
        }
        
        @Override
        public int compareTo(LemmaKey other) {
            int cmp = this.bookId.compareTo(other.bookId);
            if (cmp != 0) {
                return cmp;
            }
            
            cmp = this.lemma.compareTo(other.lemma);
            if (cmp != 0) {
                return cmp;
            }
            
            return Integer.compare(this.year, other.year);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof LemmaKey) {
                LemmaKey other = (LemmaKey) obj;
                return this.bookId.equals(other.bookId) && 
                       this.lemma.equals(other.lemma) && 
                       this.year == other.year;
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return bookId.hashCode() * 163 + lemma.hashCode() * 13 + year;
        }
        
        @Override
        public String toString() {
            return bookId + "\t" + lemma + "\t" + year;
        }
        
        // Getters
        public String getBookId() {
            return bookId;
        }
        
        public String getLemma() {
            return lemma;
        }
        
        public int getYear() {
            return year;
        }
    }
    
    private final static IntWritable ONE = new IntWritable(1);
    private StanfordCoreNLP pipeline;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize Stanford CoreNLP pipeline for lemmatization
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
        props.setProperty("tokenize.language", "en");
        props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
        
        // Setting these options to speed up processing
        props.setProperty("tokenize.options", "untokenizable=noneKeep");
        props.setProperty("ssplit.eolonly", "true");  // Only split on newlines
        
        pipeline = new StanfordCoreNLP(props);
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Parse input line from Task 1 output
            // Format: (bookId,year) cleaned_text
            String line = value.toString();
            
            // Extract bookId and year from the key part (bookId,year)
            int openBracketIndex = line.indexOf('(');
            int closeBracketIndex = line.indexOf(')');
            
            if (openBracketIndex == -1 || closeBracketIndex == -1) {
                // Skip malformed lines
                return;
            }
            
            String keyPart = line.substring(openBracketIndex + 1, closeBracketIndex);
            String[] keyParts = keyPart.split(",");
            
            if (keyParts.length != 2) {
                // Skip malformed keys
                return;
            }
            
            String bookId = keyParts[0].trim();
            int year;
            
            try {
                year = Integer.parseInt(keyParts[1].trim());
            } catch (NumberFormatException e) {
                // Skip records with invalid years
                return;
            }
            
            // Get the cleaned text part (after the closing bracket and tab)
            String cleanedText = line.substring(closeBracketIndex + 1).trim();
            
            // Apply lemmatization using Stanford CoreNLP
            CoreDocument document = new CoreDocument(cleanedText);
            pipeline.annotate(document);
            
            // Process each sentence and token to extract lemmas
            for (CoreSentence sentence : document.sentences()) {
                for (String lemma : sentence.lemmas()) {
                    // Filter out non-alphabetic lemmas and single characters
                    if (lemma.matches("[a-zA-Z]+") && lemma.length() > 1) {
                        // Create composite key (bookId, lemma, year)
                        LemmaKey outputKey = new LemmaKey(bookId, lemma.toLowerCase(), year);
                        
                        // Emit key-value pair with count 1
                        context.write(outputKey, ONE);
                    }
                }
            }
        } catch (Exception e) {
            // Log error and continue processing other records
            System.err.println("Error processing input: " + value);
            e.printStackTrace();
        }
    }
}