import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import Types.TaggedIntValue;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
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

public class Prob6 {

    public static class MapperT extends Mapper<LongWritable, Text, IntWritable, TaggedIntValue> {
        private IntWritable group = new IntWritable();
        private IntWritable rGroup = new IntWritable();
        private IntWritable t = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //split by key = group rgroup value= t
            String[] args = value.toString().split("\\t");
            //split group rgroup
            String[] groupRgroup = args[0].split(" ");
            if (args.length > 1) {
                group.set(Integer.parseInt(groupRgroup[0]));
                rGroup.set(Integer.parseInt(groupRgroup[1]));
                t.set(Integer.parseInt(args[1]));
                int notGroup = group.get() ^ 1;
                context.write(rGroup, new TaggedIntValue(new Text("T" + group.toString() + String.valueOf(notGroup)), t));
            }
        }
    }

    public static class MapperN extends Mapper<LongWritable, Text, IntWritable, TaggedIntValue> {
        private IntWritable group = new IntWritable();
        private IntWritable rGroup = new IntWritable();
        private IntWritable n = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //split by key = group rgroup value= n
            String[] args = value.toString().split("\\t");
            String[] groupRgroup = args[0].split(" ");
            if (args.length > 1) {
                group.set(Integer.parseInt(groupRgroup[0]));
                rGroup.set(Integer.parseInt(groupRgroup[1]));
                n.set(Integer.parseInt(args[1]));
                context.write(rGroup, new TaggedIntValue(new Text("N" + group.toString()), n));
            }
        }
    }

    public static class ReducerGroupWithR extends Reducer<IntWritable, TaggedIntValue,IntWritable,Text> {
        @Override
        public void reduce(IntWritable key, Iterable<TaggedIntValue> values, Context context) throws IOException,  InterruptedException {
            IntWritable t01 = new IntWritable();
            IntWritable t10 = new IntWritable();
            IntWritable n0 = new IntWritable();
            IntWritable n1 = new IntWritable();
            Text result = new Text();
            String valueTag;
            //get the N from step 1
            long N = Long.parseLong(nReader());
            //distinguish between the values by tag
            for (TaggedIntValue value : values) {
                valueTag =  value.getTag().toString();
                if(valueTag.equals("T01")){
                    t01.set(value.getValue().get());

                }else if(valueTag.equals("T10")){
                    t10.set(value.getValue().get());

                }else if(valueTag.equals("N0")){
                    n0.set(value.getValue().get());

                }else if(valueTag.equals("N1")){

                    n1.set(value.getValue().get());
                }
            }
            //calculate P
            long numerator = (t01.get()+t10.get());
            long denominator = (N*(n0.get()+n1.get()));
            double tempResult = ((double)numerator/(double)denominator);
            result.set(String.valueOf(tempResult));
            //key = R value = P
            context.write(key ,result);
        }
    }

    public static class PartitionerGroupWithR extends Partitioner<IntWritable, TaggedIntValue> {
        @Override
        public int getPartition(IntWritable key, TaggedIntValue value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Prob 6");
        job.setJarByClass(Prob6.class);
        job.setPartitionerClass(PartitionerGroupWithR.class);
        job.setReducerClass(ReducerGroupWithR.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapperT.class);
        job.setMapperClass(MapperN.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TaggedIntValue.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TaggedIntValue.class);
        //args[0] = input from step 4
        //args[1] = input from step 5
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperT.class );
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperN.class );

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static String nReader(){
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object object = s3.getObject(new GetObjectRequest(AWSservice.N_FILE_BUCKET, "N.txt"));
        InputStream content = object.getObjectContent();
        Scanner s = new Scanner(content).useDelimiter("\\A");
        String result = s.hasNext() ? s.next() : "";
        return result;
    }
}