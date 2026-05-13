package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.invertedMapper.InvertedMapper;
import org.example.invertedMapper.InvertedReducer;
import org.example.offset.OffsetMapper;
import org.example.offset.OffsetReducer;
import org.example.splitCounter.SplitCounterMapper;
import org.example.splitCounter.SplitCounterReducer;

import java.net.URI;

public class InvertedIndexDriver {

    public static void main(String[] args) throws Exception {


        String inputPath = "/input";
        String tmp1Path = "/tmp_line_counts";
        String tmp2Path = "/tmp_offsets";
        String outputPath = "/output";

        Configuration conf = new Configuration();


        Job job1 = Job.getInstance(conf, "job1-line-count");
        job1.setJarByClass(InvertedIndexDriver.class);

        job1.setMapperClass(SplitCounterMapper.class);
        job1.setReducerClass(SplitCounterReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tmp1Path));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }


        Job job2 = Job.getInstance(conf, "job2-offsets");
        job2.setJarByClass(InvertedIndexDriver.class);

        job2.setMapperClass(OffsetMapper.class);
        job2.setReducerClass(OffsetReducer.class);


        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(tmp1Path));
        FileOutputFormat.setOutputPath(job2, new Path(tmp2Path));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }


        Job job3 = Job.getInstance(conf, "job3-inverted-index");
        job3.setJarByClass(InvertedIndexDriver.class);

        job3.setMapperClass(InvertedMapper.class);
        job3.setReducerClass(InvertedReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(inputPath));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));


        job3.addCacheFile(new URI("stopwords.txt"));

        job3.addCacheFile(new URI(tmp2Path + "/part-r-00000"));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}