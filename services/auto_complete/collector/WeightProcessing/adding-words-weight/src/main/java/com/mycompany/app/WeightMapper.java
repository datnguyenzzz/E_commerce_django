package com.mycompany.app;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;

public class WeightMapper extends Mapper<
    AvroKey<Word>, //input key 
    NullWritable, //input value
    Text, //output key
    IntWritable //output value
> {
    private IntWritable weight = new IntWritable(1);

    public void map(AvroKey<Word> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        
        Text word = new Text(key.datum().getContext().toString());
        context.write(word, weight);
    }
}
