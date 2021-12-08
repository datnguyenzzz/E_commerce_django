package com.mycompany.app;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WeightCombiner extends Reducer<
    Text, 
    LongWritable,
    Text,
    LongWritable
> {
    private LongWritable weightSumUp = new LongWritable();

    public void reduce(Text word, Iterable<LongWritable> weights, Context context)
            throws IOException, InterruptedException {
        long weightSum = 0L;
        for (LongWritable w: weights) {
            weightSum += w.get();
        }
        weightSumUp.set(weightSum);
        context.write(word, weightSumUp);
    }
}
