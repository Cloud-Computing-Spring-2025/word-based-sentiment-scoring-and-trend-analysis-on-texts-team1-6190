package com.project.task1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BookMapper extends Mapper<Object, Text, Text, Text> {

    private StringBuilder textCollector = new StringBuilder();
    private String documentName = "";
    private final static Text outputKey = new Text();
    private final static Text outputValue = new Text();
    private Set<String> filterWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize filter words.
         String[] filterArray = {
    "the", "and", "a", "an", "in", "of", "to", "is", "are", "was", "were",
    "it", "this", "that", "on", "for", "with", "as", "by", "at", "from",
    "or", "but", "not", "be", "have", "has", "had", "i", "you", "he", "she",
    "they", "we", "me", "him", "her", "them", "my", "your", "their", "our",
    "so", "do", "does", "did", "will", "would", "can", "could", "just", "about",
    "into", "than", "then", "out", "up", "down", "over", "under", "chapter", "ii",
    "iii", "iv", "v", "vi", "vii", "viii", "ix", "x", "xi", "xii",
    "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx", "xxi", "xxii", "xxiii", "xxiv",
    "xxv", "xxvi", "xxvii", "xxviii", "xxix", "xxx", "xxxi", "xxxii", "xxxiii", "xxxiv", "xxxv",
    "xxxvi", "xxxvii", "xxxviii", "xxxix", "xl", "xli", "xlii", "xliii", "xliv", "xlv", "xlvi", "xlvii", "xlviii", "xlix", "l"
};

        for (String word : filterArray) {
            filterWords.add(word);
        }
        // Retrieve the document name from the input split.
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        documentName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Accumulate each line from the file.
        textCollector.append(value.toString()).append("\n");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Process the complete file content.
        String documentContents = textCollector.toString();
        if (documentContents.isEmpty()) {
            return;
        }
        // Extract metadata: Title and Release Year.
        String bookTitle = findTitle(documentContents);
        String publicationYear = findYear(documentContents);
        if (bookTitle.isEmpty()) {
            bookTitle = documentName;
        }
        if (publicationYear.isEmpty()) {
            publicationYear = "unknown";
        }
        String volumeID = findBookID(documentName);

        // Clean the text.
        String processedText = processText(documentContents);

        // Set composite key as "volumeID_publicationYear" and value as "bookTitle[TAB]processedText".
        outputKey.set(volumeID + "_" + publicationYear + "_" + bookTitle);
        outputValue.set(processedText);
        context.write(outputKey, outputValue);
    }

    // Look for a line starting with "Title:" to extract the title.
    private String findTitle(String documentContents) {
        Pattern titlePattern = Pattern.compile("Title:\\s*(.*)");
        Matcher titleMatcher = titlePattern.matcher(documentContents);
        if (titleMatcher.find()) {
            return titleMatcher.group(1).trim().toLowerCase();
        }
        return "";
    }

    // Look for a line starting with "Release date:" and extract the first 4-digit number.
    private String findYear(String documentContents) {
        Pattern datePattern = Pattern.compile("Release date:\\s*(.*)");
        Matcher dateMatcher = datePattern.matcher(documentContents);
        if (dateMatcher.find()) {
            String dateInfo = dateMatcher.group(1);
            Pattern yearFinder = Pattern.compile("\\b(\\d{4})\\b");
            Matcher yearMatcher = yearFinder.matcher(dateInfo);
            if (yearMatcher.find()) {
                return yearMatcher.group(1);
            }
        }
        return "";
    }

    // Extract book ID from the file name (e.g., "pg245.txt" -> "245").
    private String findBookID(String documentName) {
        Pattern idPattern = Pattern.compile("pg(\\d+)\\.txt");
        Matcher idMatcher = idPattern.matcher(documentName);
        if (idMatcher.find()) {
            return idMatcher.group(1);
        }
        return "unknown";
    }

    // Clean the text: convert to lowercase, remove punctuation, and filter out stop words.
    private String processText(String rawText) {
        // Lowercase and remove non-alphanumeric characters.
        String lowercaseText = rawText.toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
        StringBuilder cleanTextBuilder = new StringBuilder();
        for (String word : lowercaseText.split("\\s+")) {
            if (!filterWords.contains(word) && !word.isEmpty()) {
                cleanTextBuilder.append(word).append(" ");
            }
        }
        return cleanTextBuilder.toString().trim();
    }
}