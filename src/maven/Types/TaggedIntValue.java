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
public class TaggedIntValue implements Writable {

    Text tag;
    IntWritable value;

    //Default Constructor
    public TaggedIntValue() {
        this.tag = new Text();
        this.value = new IntWritable();
    }

    //Default Constructor
    public TaggedIntValue(Text tag, IntWritable value) {
        this.tag = tag;
        this.value = value;
    }

    public void set(Text tag, IntWritable value) {
        this.tag = tag;
        this.value = value;
    }

    public Text getTag() {
        return tag;
    }

    public IntWritable getValue() {
        return value;
    }

    public void write(DataOutput dataOutput) throws IOException {
        tag.write(dataOutput);
        value.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        tag.readFields(dataInput);
        value.readFields(dataInput);
    }

    public String toString() {
        return (tag.toString() +" : " + value.toString());
    }
}