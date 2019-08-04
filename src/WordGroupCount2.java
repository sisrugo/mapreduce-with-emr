import java.io.IOException;
import java.util.StringTokenizer;

import Types.WordGroup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordGroupCount2 {

    public static class MapperGroupCount extends Mapper<LongWritable, Text, WordGroup, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private IntWritable group = new IntWritable();
        WordGroup wg = new WordGroup();

        @Override
        protected void setup(Context context){
            this.group.set(0);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] tokens = value.toString().split("\\t");
            if(tokens.length > 0 && checkiflegalWord(tokens[0])) {
                word.set(tokens[0]);
                //divide the words to group (0\1)
                group.set(group.get()^1);
                wg.set(word, group);
                //key = word,group value=1
                context.write(wg, one);
            }

        }
    }

    public static class ReducerGroupCount extends Reducer<WordGroup,IntWritable,WordGroup,IntWritable> {
        @Override
        public void reduce(WordGroup key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerGroupCount extends Partitioner<WordGroup, IntWritable> {
        @Override
        public int getPartition(WordGroup key, IntWritable value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word groups count 2");
        job.setJarByClass(WordGroupCount2.class);
        job.setMapperClass(MapperGroupCount.class);
        job.setPartitionerClass(PartitionerGroupCount.class);
        job.setCombinerClass(ReducerGroupCount.class);
        job.setReducerClass(ReducerGroupCount.class);

        job.setMapOutputKeyClass(WordGroup.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(WordGroup.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static boolean checkiflegalWord(String word) {
        //accept only hebrew letters 1487-1513 in ASCII or space
        for (int i = 0; i < word.length(); i++) {
            if(((int)word.charAt(i) < 1488 || (int)word.charAt(i) > 1514) && (int)word.charAt(i) != 32 )
                return false;
        }
        return true;

    }
}