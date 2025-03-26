package com.project.task2;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.PropertiesUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TermExtractionMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text outputKey = new Text();
    private static final IntWritable singleOccurrence = new IntWritable(1);
    private static StanfordCoreNLP nlpProcessor;

    @Override
    protected void setup(Context context) {
        if (nlpProcessor == null) {
            Properties nlpProperties = PropertiesUtils.asProperties(
                "annotators", "tokenize,ssplit,pos,lemma",
                "tokenize.language", "en",
                "ssplit.isOneSentence", "true"
            );
            nlpProcessor = new StanfordCoreNLP(nlpProperties);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString().trim();
        if (inputLine.isEmpty()) return;

        String[] contentSegments = inputLine.split("\t", 2);
        if (contentSegments.length < 2) return;

        String metadataSection = contentSegments[0].trim();
        String[] metadataElements = metadataSection.split("_", 3);
        if (metadataElements.length < 3) return;

        String volumeId = metadataElements[0].trim();
        String publicationYear = metadataElements[1].trim();
        String contentSection = contentSegments[1].trim();

        CoreDocument processedDocument = new CoreDocument(contentSection);
        nlpProcessor.annotate(processedDocument);

        List<CoreLabel> wordTokens = processedDocument.tokens();
        for (CoreLabel token : wordTokens) {
            String normalizedWord = token.lemma();
            if (normalizedWord != null && !normalizedWord.isEmpty()) {
                normalizedWord = normalizedWord.toLowerCase();
                if (normalizedWord.matches("[a-zA-Z]+")) {  // filter out numbers and symbols
                    outputKey.set(volumeId + ", " + normalizedWord + ", " + publicationYear);
                    context.write(outputKey, singleOccurrence);
                }
            }
        }
    }
}