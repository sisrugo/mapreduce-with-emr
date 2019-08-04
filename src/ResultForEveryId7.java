import java.io.IOException;
import Types.TaggedIntKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ResultForEveryId7 {

    public static class MapperResult extends Mapper<LongWritable, Text, TaggedIntKey, Text> {
        private Text R = new Text();
        private Text result = new Text();
        private TaggedIntKey ti = new TaggedIntKey();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //split by key value
            String[] args = value.toString().split("\\t");
            if(args.length > 1) {
                R.set(args[0]);
                result.set(args[1]);
                ti.set(R, new IntWritable(0));
                //key = R,0 (the 0 is to ensure the P will be the first value in the reducer) value = P
                context.write(ti, result);
            }
        }
    }

    public static class MapperId extends Mapper<LongWritable, Text, TaggedIntKey, Text> {
        private Text word = new Text();
        private Text R = new Text();
        private TaggedIntKey ti = new TaggedIntKey();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //split by key value
            String[] args = value.toString().split("\\t");
            if(args.length > 1) {
                word.set(args[0]);
                R.set(args[1]);
                ti.set(R, new IntWritable(1));
                //key = R,1  (the 0 is to ensure the word will be value after Probability in the reducer) value = word
                context.write(ti, word);
            }
        }
    }

    public static class ReducerResult extends Reducer<TaggedIntKey, Text,Text,Text> {
        Text result = new Text("0");
        Text lastR = new Text();

        @Override
        //unit the word with probability
        public void reduce(TaggedIntKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            // assume that according to our partitioner all keys with same R will be in the same reducer
            // according to the key second value (0/1), first will be the result (with the value 0) and than all word (with the value 1)
            for (Text value : values) {
                if(key.getIntKey().equals(new IntWritable(0))){
                    result.set(value.toString());
                    lastR.set(key.getTextKey().toString());
                }else{
                    if(!key.getTextKey().equals(lastR)){
                     result.set("0");
                    }
                    context.write(value , result);
                }
            }
        }
    }

    public static class PartitionerGroupWithR extends Partitioner<TaggedIntKey, Text> {
        @Override
        public int getPartition(TaggedIntKey key, Text value, int numPartitions) {
            // ensure the same R will be in the same reducer
            return (key.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "result for every id 7");
        job.setJarByClass(ResultForEveryId7.class);
        job.setPartitionerClass(PartitionerGroupWithR.class);
        job.setReducerClass(ReducerResult.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapperId.class);
        job.setMapperClass(MapperResult.class);

        job.setMapOutputKeyClass(TaggedIntKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(TaggedIntKey.class);
        job.setMapOutputValueClass(Text.class);
        //arg[0] = input from step 1
        //arg[1] = input from step 6
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperId.class ); //from step 1
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperResult.class ); // from step 6

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}