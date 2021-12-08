package com.mycompany.app;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;

public class WeightMapper extends Mapper<
    AvroKey<Word>, //input key 
    NullWritable, //input value
    Text, //output key
    LongWritable //output value
> {
    private static final String BASE_WEIGHT_KEY = "base_weight";
    private LongWritable weight;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        this.weight = new LongWritable(context.getConfiguration().getInt(BASE_WEIGHT_KEY,1));
    }

    public void map(AvroKey<Word> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        
        Text word = new Text(key.datum().getContext().toString());
        context.write(word, weight);
    }
}
