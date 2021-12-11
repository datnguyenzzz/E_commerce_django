import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeightSort extends Configured implements Tool {

    public static class WeightSortMapper extends Mapper<
        AvroKey<CharSequence>,
        AvroValue<Long>,
        LongWritable,
        Text
    > {
        public void map(AvroKey<CharSequence> key, AvroValue<Long> value, Context context) 
                throws IOException, InterruptedException {
            LongWritable weight = new LongWriable(value.datum()); 
            Text stringKey = new Text(key.datum().toString());

            context.write(weight, stringKey);
        }
    }

    public static class WeightSortReducer extends Reducer<
        LongWritable,
        Text, 
        LongWritable,
        Text
    > {
        public void reduce(LongWritable weight, Iterable<Text> words, Context context) 
                throws IOException, InterruptedException {
            for (Text w : words) {
                context.write(weight, w);
            }
        }
    }
 
    public static class DescendingOrder extends WritableComparator {
        protected DescendingOrder() {
            super(LongWritable.class, true);
        }

        @Override 
        public int compare(WritableComparable a, WritableComparable b) {
            LongWritable long_a = (LongWritable) a; 
            LongWritable long_b = (LongWritable) b; 

            return -1 * long_a.compareTo(long_b);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WeightSort(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WeightSort <input> <output>");
            System.exit(2);
        }
        
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "weight sort");
        job.setJarByClass(WeightSort.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(WeightSortMapper.class);
        job.setReducerClass(WeightSortReducer.class);
        job.setSortComparatorClass(DescendingOrder.class);

        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Schema.create(Schema.Type.LONG));

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}