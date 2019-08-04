package Types; /**
 * Created by sharo on 12/13/2018.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupRgroup implements WritableComparable<GroupRgroup>{
    IntWritable group;
    IntWritable rGroup;

    //Default Constructor
    public GroupRgroup() {
        this.rGroup = new IntWritable();
        this.group = new IntWritable();
    }

    //Custom Constructor
    public GroupRgroup(IntWritable group, IntWritable rGroup) {
        this.group = group;
        this.rGroup = rGroup;
    }

    public void set(IntWritable group, IntWritable rGroup) {
        this.group = group;
        this.rGroup = rGroup;
    }
    public IntWritable getrGroup() {
        return rGroup;
    }

    public IntWritable getGroup() {
        return group;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        group.write(dataOutput);
        rGroup.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        group.readFields(dataInput);
        rGroup.readFields(dataInput);
    }

    @Override
    public int compareTo(GroupRgroup other) {
        int groupCompare = this.group.compareTo(other.getGroup());
        if (groupCompare != 0)
            return groupCompare;
        return rGroup.compareTo(other.getrGroup());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GroupRgroup){
            GroupRgroup other = (GroupRgroup) o;
            return((this.compareTo(other)==0)? true : false);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return group.hashCode() + rGroup.hashCode();
    }

    @Override
    public String toString(){
        return group.toString() +" "+ rGroup.toString();
    }
}