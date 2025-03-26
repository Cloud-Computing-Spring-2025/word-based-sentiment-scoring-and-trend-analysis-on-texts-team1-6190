package com.project.task4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookYearMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text resultKey = new Text();
    private final IntWritable resultCount = new IntWritable();

    @Override
    public void map(Object identifier, Text record, Context context) throws IOException, InterruptedException {
        String inputLine = record.toString().trim();
        if (inputLine.isEmpty()) return;

        String[] segments = inputLine.split("\t");
        if (segments.length != 2) return;

        String[] bookData = segments[0].split(",");
        if (bookData.length < 2) return;

        String titleId = bookData[0].trim();
        String publishYear = bookData[bookData.length - 1].trim();

        try {
            int yearNum = Integer.parseInt(publishYear);
            String decadeLabel = (yearNum / 10 * 10) + "s";

            // Output format: "titleId,decadeLabel"
            resultKey.set(titleId + "," + decadeLabel);
            resultCount.set(Integer.parseInt(segments[1].trim()));

            context.write(resultKey, resultCount);
        } catch (NumberFormatException ex) {
            // Skip entries with invalid number format
        }
    }
}