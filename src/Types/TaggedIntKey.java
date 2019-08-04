package Types;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sharo on 12/18/2018.
 */
public class TaggedIntKey implements WritableComparable<TaggedIntKey> {

    Text textKey;
    IntWritable intKey;

    //Default Constructor
    public TaggedIntKey() {
        this.textKey = new Text();
        this.intKey = new IntWritable();
    }

    //Default Constructor
    public TaggedIntKey(Text textKey, IntWritable intKey) {
        this.textKey = textKey;
        this.intKey = intKey;
    }

    public void set(Text textKey, IntWritable intKey) {
        this.textKey = textKey;
        this.intKey = intKey;
    }

    public Text getTextKey() {
        return textKey;
    }

    public IntWritable getIntKey() {
        return intKey;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        textKey.write(dataOutput);
        intKey.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        textKey.readFields(dataInput);
        intKey.readFields(dataInput);
    }

    @Override
    public String toString() {
        return (textKey.toString() +" , " + intKey.toString());
    }

    @Override
    public int compareTo(TaggedIntKey other) {
        int resultCompare = textKey.compareTo(other.getTextKey());
        if(resultCompare == 0 )
            return this.intKey.compareTo(other.getIntKey());
        return resultCompare;
    }

    @Override
    public int hashCode(){
        return textKey.hashCode();
    }
}
