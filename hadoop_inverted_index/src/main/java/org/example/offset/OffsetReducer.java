package org.example.offset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class OffsetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private String currentFile = "";
    private int cumulativeOffset = 0;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String keyStr = key.toString();
        String fileName = keyStr.substring(0, keyStr.lastIndexOf("-"));

        if (!fileName.equals(currentFile)) {
            currentFile = fileName;
            cumulativeOffset = 0;
        }

        int sumOfLinesInThisSplit = 0;
        for (IntWritable v : values) {
            sumOfLinesInThisSplit += v.get();
        }

        context.write(key, new IntWritable(cumulativeOffset));


        cumulativeOffset += sumOfLinesInThisSplit;
    }
}