package org.example.invertedMapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public class InvertedReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, TreeSet<Integer>> index = new TreeMap<>();
        for (Text val : values) {
            String[] p = val.toString().split(":");
            index.computeIfAbsent(p[0], k -> new TreeSet<>()).add(Integer.parseInt(p[1]));
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, TreeSet<Integer>> e : index.entrySet()) {
            sb.append("(").append(e.getKey());
            for (Integer line : e.getValue()) sb.append(", line#").append(line);
            sb.append(") ");
        }
        context.write(key, new Text(sb.toString().trim()));
    }
}
