package com.mycompany.app;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

// Text,long -> AvroKey, AvroLong 
public class WeightReducer extends Reducer<
    Text, 
    LongWritable,
    AvroKey<CharSequence>,
    AvroValue<Long>
> {
    public void reduce(Text word, Iterable<LongWritable> weights, Context context)
            throws IOException, InterruptedException {
        long weightSum = 0L;
        for (LongWritable w: weights) {
            weightSum += w.get();
        }

        AvroKey marshalled_word = new AvroKey<CharSequence>(word.toString()); 
        AvroValue marshalled_value = new AvroValue<Long>(weightSum);
        context.write(marshalled_word, marshalled_value);
    }
}