import java.io.IOException;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

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
public class WeightCompute extends Configured implements Tool {

    private static final String BASE_WEIGHT_KEY = "base_weight";

    public static class WeightMapper extends Mapper<
        AvroKey<com.example.GatheringService.Word>, //input key 
        NullWritable, //input value
        Text, //output key
        LongWritable //output value
    > {
        private LongWritable weight;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.weight = new LongWritable(context.getConfiguration().getInt(BASE_WEIGHT_KEY,1));
        }

        public void map(AvroKey<com.example.GatheringService.Word> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            
            Text word = new Text(key.datum().getContext().toString());
            context.write(word, this.weight);
        }
    }
    /*
    public static class WeightCombiner extends Reducer<
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
    */
    public static class WeightReducer extends Reducer<
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
            context.write(new AvroKey<CharSequence>(word.toString()), new AvroValue<Long>(weightSum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: WeightCompute <input> <output> <weight>");
            System.out.println(args[0]);
            System.out.println(args[1]);
            System.exit(2);
        }
        
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        Integer baseWeight = Integer.valueOf(args[2]);

        Job job = Job.getInstance(getConf(), "weight compute");
        job.setJarByClass(WeightCompute.class);
        job.getConfiguration().set("avro.mo.config.namedOutput", inPath.getName());
        job.getConfiguration().setInt(BASE_WEIGHT_KEY, baseWeight);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(WeightMapper.class);
        //job.setCombinerClass(WeightCombiner.class);
        job.setReducerClass(WeightReducer.class);

        AvroJob.setInputKeySchema(job, com.example.GatheringService.Word.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.LONG));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main( String[] args ) throws Exception {
        int result = ToolRunner.run(new Configuration(), new WeightCompute(), args);
        System.exit(result);
    }
}
