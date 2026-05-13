package org.example.offset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OffsetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text split = new Text();
    private IntWritable count = new IntWritable();


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        split.set(parts[0]);
        count.set(Integer.parseInt(parts[1]));

        context.write(split, count);
    }
}