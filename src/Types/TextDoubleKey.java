package Types; /**
 * Created by sharo on 12/13/2018.
 */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextDoubleKey implements WritableComparable<TextDoubleKey>{
    Text textKey;
    DoubleWritable doubleKey;

    //Default Constructor
    public TextDoubleKey() {
        this.textKey = new Text();
        this.doubleKey = new DoubleWritable();
    }

    //Custom Constructor
    public TextDoubleKey(Text textKey, DoubleWritable doubleKey) {
        this.textKey = textKey;
        this.doubleKey = doubleKey;
    }

    //Setter method to set the values of Types.TextIntKey object
    public void set(Text word, DoubleWritable doubleKey) {
        this.textKey = word;
        this.doubleKey = doubleKey;
    }

    public Text getTextKey() {
        return textKey;
    }

    public DoubleWritable getDoubleKey() {
        return doubleKey;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        textKey.write(dataOutput);
        doubleKey.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        textKey.readFields(dataInput);
        doubleKey.readFields(dataInput);
    }

    @Override
    public int compareTo(TextDoubleKey other) {
        int wordCompare = this.getTextKey().compareTo(other.getTextKey());
        if (wordCompare != 0)
            return wordCompare;
        return (-1) * doubleKey.compareTo(other.getDoubleKey()); // for descending order
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextDoubleKey){
            TextDoubleKey other = (TextDoubleKey) o;
            return((this.compareTo(other)==0)? true : false);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return textKey.hashCode() + doubleKey.hashCode();
    }

    @Override
    public String toString(){
        return textKey.toString() +" "+ doubleKey.toString();
    }
}