package com.mycompany.app;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool {

    private static final String BASE_WEIGHT_KEY = "base_weight";

    @Override 
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: App <input> <output> <weight>");
            System.exit(-1);
        }
        
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        Integer baseWeight = Integer.valueOf(args[2]);

        Job job = Job.getInstance(getConf(), "Weight Compute");
        job.setJarByClass(App.class);
        job.getConfiguration().set("avro.mo.config.namedOutput", inPath.getName());
        job.getConfiguration().setInt(BASE_WEIGHT_KEY, baseWeight);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(WeightMapper.class);
        job.setCombinerClass(WeightCombiner.class);
        job.setReducerClass(WeightReducer.class);

        AvroJob.setInputKeySchema(job, Word.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.LONG));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main( String[] args ) throws Exception {
        int result = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(result);
    }
}
