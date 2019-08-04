import java.io.IOException;
import Types.TaggedIntValue;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniteWordGroupWithR3 {

    public static class MapperGroup2 extends Mapper<LongWritable, Text, WordGroup, TaggedIntValue> {
        private Text word = new Text();
        private IntWritable group = new IntWritable();
        private IntWritable rGroup = new IntWritable();
        private WordGroup wg = new WordGroup();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //split by key value
            //arg[0] = word-group arg[1] = rgroup
            String[] args = value.toString().split("\\t");
            // split the wordgroup
            //groupword[0] = word groupword[1]= group
            String[] groupWord = args[0].split("-");
            if(args.length > 1) {
                word.set(groupWord[0]);
                group.set(Integer.parseInt(groupWord[1]));
                rGroup.set(Integer.parseInt(args[1]));
                wg.set(word, group);
                context.write(wg, new TaggedIntValue(new Text("G"), group));
                context.write(wg, new TaggedIntValue(new Text("RG"), rGroup));
            }

        }
    }

    public static class MapperR1 extends Mapper<LongWritable, Text, WordGroup, TaggedIntValue> {
        private Text word = new Text();
        private IntWritable R = new IntWritable();
        private WordGroup wg = new WordGroup();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //SPLIT BY TYPE
            //arg[0] = word arg[1] = R
            String[] args = value.toString().split("\\t");
            if(args.length > 1) {
                word.set(args[0]);
                R.set(Integer.parseInt(args[1]));
                //write for each group word and R
                wg.set(word, new IntWritable(0));
                context.write(wg, new TaggedIntValue(new Text("R"), R));
                wg.set(word, new IntWritable(1));
                context.write(wg, new TaggedIntValue(new Text("R"), R));
            }
        }
    }

    public static class ReducerGroupWithR extends Reducer<WordGroup, TaggedIntValue,Text,Text> {
        @Override
        public void reduce(WordGroup key, Iterable<TaggedIntValue> values, Context context) throws IOException,  InterruptedException {
            IntWritable R = new IntWritable();
            IntWritable group = new IntWritable(-1);
            IntWritable rGroup = new IntWritable();
            //distinguish between the values by tag
            for (TaggedIntValue value : values) {
                switch (value.getTag().toString()){
                    case "R": R.set(value.getValue().get());
                        break;
                    case "G": group.set(value.getValue().get());
                        break;
                    case "RG": rGroup.set(value.getValue().get());
                        break;
                }
            }
            if (group.get() != -1){
                //write the values for each word
                Text result = new Text(R.toString()+" "+group.toString()+" "+rGroup.toString());
                context.write(key.getWord(), result);
            }
        }
    }

    public static class PartitionerGroupWithR extends Partitioner<WordGroup, TaggedIntValue> {
        @Override
        public int getPartition(WordGroup key, TaggedIntValue value, int numPartitions) {
            return (Math.abs(key.hashCode()) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "unit R group rGroup - 3");
        job.setJarByClass(UniteWordGroupWithR3.class);
        job.setPartitionerClass(PartitionerGroupWithR.class);
        job.setReducerClass(ReducerGroupWithR.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapperR1.class);
        job.setMapperClass(MapperGroup2.class);

        job.setMapOutputKeyClass(WordGroup.class);
        job.setMapOutputValueClass(TaggedIntValue.class);
        job.setMapOutputKeyClass(WordGroup.class);
        job.setMapOutputValueClass(TaggedIntValue.class);
        //arg0 = output from step 1
        //arg1 = output from step 2
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperR1.class );
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperGroup2.class );

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}