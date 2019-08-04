import java.io.IOException;
import java.util.StringTokenizer;

import Types.GroupRgroup;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ncalc5 {

    public static class MapperClass extends Mapper<LongWritable, Text, GroupRgroup, IntWritable> {
        private GroupRgroup grg = new GroupRgroup();
        private IntWritable group = new IntWritable();
        private IntWritable rGroup = new IntWritable();
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // split by key value
            //arg[0] = word-group arg[1] = rgroup
            String[] args = value.toString().split("\\t");
            //split wordgroup by fields
            //groupword[0] = word groupword[1]= group
            String[] groupWord = args[0].split("-");
            if(args.length > 1) {
                group.set(Integer.parseInt(groupWord[1]));
                rGroup.set(Integer.parseInt(args[1]));
                grg.set(group, rGroup);
                //key = group, rgroup value = 1
                context.write(grg, one);
            }
        }
    }

    public static class ReducerClass extends Reducer<GroupRgroup,IntWritable,GroupRgroup,IntWritable> {
        @Override
        //count how many times a word is in a group
        public void reduce(GroupRgroup key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<GroupRgroup, IntWritable> {
        @Override
        public int getPartition(GroupRgroup key, IntWritable value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "N calc 5");
        job.setJarByClass(Ncalc5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(GroupRgroup.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(GroupRgroup.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}