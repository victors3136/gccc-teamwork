package org.example.invertedMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class InvertedMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, Integer> offsets = new HashMap<>();
    private Set<String> stopwords = new HashSet<>();
    private String fileName;
    private int baseOffset = 0;
    private int localLine = 0;

    @Override
    protected void setup(Context context) throws IOException {
        FileSplit split = (FileSplit) context.getInputSplit();
        this.fileName = split.getPath().getName();
        String lookupKey = fileName + "-" + String.format("%015d", split.getStart());

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null) {
            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                if (path.getName().contains("stopwords")) readStopwords(path.getName());
                else if (path.getName().contains("part-r-00000")) readOffsets(path.getName());
            }
        }
        this.baseOffset = offsets.getOrDefault(lookupKey, 0);
    }

    private void readStopwords(String p) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(p));
        String l; while ((l = br.readLine()) != null) stopwords.add(l.trim().toLowerCase());
        br.close();
    }

    private void readOffsets(String p) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(p));
        String l; while ((l = br.readLine()) != null) {
            String[] parts = l.split("\\s+");
            if (parts.length == 2) offsets.put(parts[0], Integer.parseInt(parts[1]));
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        localLine++;
        int globalLine = baseOffset + localLine;
        String line = value.toString().toLowerCase().replaceAll("[^a-z ]", " ");
        for (String w : line.split("\\s+")) {
            if (!w.isEmpty() && !stopwords.contains(w)) {
                context.write(new Text(w), new Text(fileName + ":" + globalLine));
            }
        }
    }
}
