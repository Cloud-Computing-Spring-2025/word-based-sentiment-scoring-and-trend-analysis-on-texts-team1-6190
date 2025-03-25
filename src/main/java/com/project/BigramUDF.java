package com.project;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class BigramUDF extends UDF {
    public Map<String,DoubleWritable> evaluate(List<String> words, List<Integer> counts) {
        Map<String,DoubleWritable> bigramCounts = new HashMap<>();
        if (words == null || words.size() < 2) return bigramCounts;
        
        for(int i=0; i<words.size()-1; i++) {
            String bigram = words.get(i) + "_" + words.get(i+1);
            int minCount = Math.min(counts.get(i), counts.get(i+1));
            bigramCounts.put(bigram, new DoubleWritable(minCount));
        }
        return bigramCounts;
    }
}