package org.example.splitCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class SplitCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text splitKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();
        String formattedStart = String.format("%015d", split.getStart());
        splitKey.set(fileName + "-" + formattedStart);

        context.write(splitKey, one);
    }
}
