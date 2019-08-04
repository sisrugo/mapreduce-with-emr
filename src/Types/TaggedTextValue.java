package Types;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sharo on 12/18/2018.
 */
public class TaggedTextValue implements Writable{

    Text tag;
    Text value;

    //Default Constructor
    public TaggedTextValue() {
        this.tag = new Text();
        this.value = new Text();
    }

    //Default Constructor
    public TaggedTextValue(Text tag, Text value) {
        this.tag = tag;
        this.value = value;
    }

    public void set(Text tag, Text value) {
        this.tag = tag;
        this.value = value;
    }

    public Text getTag() {
        return tag;
    }

    public Text getValue() {
        return value;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        tag.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag.readFields(dataInput);
        value.readFields(dataInput);
    }

    @Override
    public String toString() {
        return (tag.toString() +" : " + value.toString());
    }
}