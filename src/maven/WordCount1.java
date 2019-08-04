import java.io.File;
import java.io.IOException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //splits the key and value
            String[] tokens = value.toString().split("\\t");
            if(checkiflegalWord(tokens[0])) {
                word.set(tokens[0]);
                //count every word at the corpus
                context.getCounter(Counter.COUNTER_N).increment(1);
                context.write(word, one);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count 1");
        job.setJarByClass(WordCount1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)) {
            long counter = job.getCounters().findCounter(Counter.COUNTER_N).getValue();
            //save the counter in s3
            uploadnFile(counter);
            System.exit(0);
        }
        System.exit(1);
    }


    private static void uploadnFile(long c) {

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        try {
            FileUtils.writeStringToFile(new File("N.txt"), String.valueOf(c));
        } catch (IOException e) {
            System.out.println("Error: can not write N to text");
        }

        s3.putObject(new PutObjectRequest(AWSservice.N_FILE_BUCKET, "N.txt", new File("N.txt")));
    }


    private static boolean checkiflegalWord(String word) {

        for (int i = 0; i < word.length(); i++) {
            if(((int)word.charAt(i) < 1488 || (int)word.charAt(i) > 1514) && (int)word.charAt(i) != 32 )
                return false;
        }
        return true;

    }
}