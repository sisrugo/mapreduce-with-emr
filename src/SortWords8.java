import java.io.IOException;
import Types.TextDoubleKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortWords8 {

    public static class MapperClass extends Mapper<LongWritable, Text, TextDoubleKey, Text> {
        private Text grams = new Text();
        private Text w1w2 = new Text();
        private DoubleWritable prob = new DoubleWritable();
        private TextDoubleKey sortKey = new TextDoubleKey();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //split by key value, arg0 = word arg1 = p
            String[] args = value.toString().split("\\t");
            //split the w1w2w3 word to w1, w2, w3
            String[] word = args[0].split(" ");
            String w1 = word[0];
            String w2 = word[1];
            String w3 = word[2];

            prob.set(Double.valueOf(args[1]));
            w1w2.set(w1+w2);
            grams.set(w1 +" "+w2 + " "+w3);

            sortKey.set(w1w2, prob);
            //sort by w1w2 and probability
            context.write(sortKey, grams);
        }
    }

    public static class ReducerClass extends Reducer<TextDoubleKey, Text, Text, DoubleWritable> {
        @Override
        public void reduce(TextDoubleKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Text grams = new Text();
            for (Text value : values) {
                grams.set(value.toString());
                context.write(grams, key.getDoubleKey());
            }
        }
    }

    public static class PartitionerClass extends Partitioner<TextDoubleKey, Text> {
        @Override
        public int getPartition(TextDoubleKey key, Text value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "sort grams 8");
        job.setJarByClass(SortWords8.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(TextDoubleKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }
}