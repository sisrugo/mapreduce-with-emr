package Types;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordGroup implements WritableComparable<WordGroup>{
    Text word;
    IntWritable group;

    //Default Constructor
    public WordGroup() {
        this.word = new Text();
        this.group = new IntWritable();
    }

    //Custom Constructor
    public WordGroup(Text word, IntWritable group) {
        this.word = word;
        this.group = group;
    }

    //Setter method to set the values of Types.WordGroup object
    public void set(Text word, IntWritable group) {
        this.word = word;
        this.group = group;
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getGroup() {
        return group;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        group.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        group.readFields(dataInput);
    }

    @Override
    public int compareTo(WordGroup other) {
        int wordCompare = this.getWord().compareTo(other.getWord());
        if (wordCompare != 0)
            return wordCompare;
        return group.compareTo(other.getGroup());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WordGroup){
            WordGroup other = (WordGroup) o;
            return((this.compareTo(other)==0)? true : false);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return word.hashCode() + group.hashCode();
    }

    @Override
    public String toString(){
        return word.toString() +"-"+ group.toString();
    }
}