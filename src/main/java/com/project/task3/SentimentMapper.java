package com.project.task3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.net.URI;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashMap<String, Integer> sentimentDictionary = new HashMap<>();
    private Text volumeYearKey = new Text();
    private IntWritable sentimentValue = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader reader = new BufferedReader(new FileReader("AFINN-111.txt"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length == 2) {
                    sentimentDictionary.put(tokens[0].toLowerCase(), Integer.parseInt(tokens[1]));
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input: volumeID, lemma, year \t count
        String[] parts = value.toString().split("\t");
        if (parts.length != 2) return;

        String[] fields = parts[0].split(",");
        if (fields.length != 3) return;

        String volumeID = fields[0].trim();
        String lemma = fields[1].trim().toLowerCase();
        String year = fields[2].trim();
        int count = Integer.parseInt(parts[1]);

        if (sentimentDictionary.containsKey(lemma)) {
            int sentimentScore = sentimentDictionary.get(lemma) * count;
            volumeYearKey.set(volumeID + "," + year);
            sentimentValue.set(sentimentScore);
            context.write(volumeYearKey, sentimentValue);
        }
    }
}