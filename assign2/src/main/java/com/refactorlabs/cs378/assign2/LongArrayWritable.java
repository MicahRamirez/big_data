import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by alpha on 2/9/15.
 */
public class LongArrayWritable extends ArrayWritable {

    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public long[] getValueArray() {
        Writable[] wValues = get();
        long[] values = new long[wValues.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = ((LongWritable) wValues[i]).get();
        }
        return values;
    }

    public LongArrayWritable sum(java.lang.Iterable<LongArrayWritable> items){
        long[] sums = new long[3];
        for(LongArrayWritable item: items){
            long[] lawArr = item.getValueArray();
            sums[0] += lawArr[0];
            sums[1] += lawArr[1];
            sums[2] += lawArr[2];
        }
        LongArrayWritable res = new LongArrayWritable();
        Writable[] temp = {new LongWritable(sums[0]), new LongWritable((sums[1])), new LongWritable(sums[2])};
        res.set(temp);
        return res;
    }
}
