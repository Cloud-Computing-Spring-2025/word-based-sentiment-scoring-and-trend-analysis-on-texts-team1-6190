

package com.project.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class BigramUDF extends UDF {
    public MapWritable evaluate(List<String> lemmaFreqList) {
        MapWritable output = new MapWritable();
        
        // Iterate through the list of lemma-frequency pairs
        for (int i = 0; i < lemmaFreqList.size(); i++) {
            String entry1 = lemmaFreqList.get(i);
            String[] split1 = entry1.split(":");
            
            if (split1.length != 2) continue;  // Skip if the split doesn't result in exactly two parts
            
            String lemma1 = split1[0].trim();
            int freq1;
            
            try {
                freq1 = Integer.parseInt(split1[1].trim());
            } catch (NumberFormatException e) {
                continue;  // Skip if the frequency can't be parsed as an integer
            }
            
            // Compare each lemma with subsequent ones to create bigrams
            for (int j = i + 1; j < lemmaFreqList.size(); j++) {
                String entry2 = lemmaFreqList.get(j);
                String[] split2 = entry2.split(":");
                
                if (split2.length != 2) continue;
                
                String lemma2 = split2[0].trim();
                int freq2;
                
                try {
                    freq2 = Integer.parseInt(split2[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }
                
                String bigram = lemma1 + " " + lemma2;
                IntWritable bigramFrequency = new IntWritable(freq1 * freq2);
                Text bigramKey = new Text(bigram);
                
                // If the bigram is already in the result, update its count
                if (output.containsKey(bigramKey)) {
                    IntWritable existingFrequency = (IntWritable) output.get(bigramKey);
                    bigramFrequency.set(existingFrequency.get() + bigramFrequency.get());
                }
                
                output.put(bigramKey, bigramFrequency);
            }
        }
        
        return output;
    }
}
