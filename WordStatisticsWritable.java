import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class WordStatisticsWritable implements Writable{

    private static final int COUNT = 3;
    private static final int STAT = 2;
    private LongArrayWritable counts;
    private DoubleArrayWritable stats;
    private int numItems;
    public WordStatisticsWritable() {
        counts = new LongArrayWritable();
        stats = new DoubleArrayWritable();
    }

    public void write(DataOutput out) throws IOException{
        long[] temp = counts.getValueArray();
        out.write(COUNT + STAT);
        for(int i = 0; i < temp.length; i++){
            out.writeLong(temp[i]);
        }
        double[] dtemp = stats.getValueArray();
        for(int i = 0; i < dtemp.length; i++){
            out.writeDouble(dtemp[i]);
        }


    }

    public void readFields(DataInput in) throws IOException{
        int numItems = in.readInt();
        LongWritable[] ltemp = new LongWritable[COUNT];
        for(int i = 0; i < ltemp.length; i++){
            ltemp[i] = new LongWritable(in.readLong());
        }
        counts.set( ltemp);

        DoubleWritable[] dtemp = new DoubleWritable[STAT];
        for(int i = 0; i < dtemp.length; i++){
            dtemp[i] = new DoubleWritable(in.readDouble());
        }
        stats.set(dtemp);
    }

    public void set(Writable[] values){
        int totalIndex;
        Writable[] longs = new Writable[COUNT];
        int longIndex = 0;
        for(totalIndex = 0; totalIndex < COUNT; totalIndex++){
            longs[longIndex] = values[totalIndex];
            longIndex++;
        }
        counts.set(longs);
        Writable[] doubles = new Writable[STAT];
        int doubleIndex = 0;
        for(totalIndex = 0 + COUNT - 1; totalIndex < COUNT+STAT; totalIndex++){
            doubles[doubleIndex] = values[totalIndex];
            doubleIndex++;
        }
        stats.set(doubles);
    }


    public long[] sumLong(Iterable<WordStatisticsWritable> sums){
        long[] valAr = counts.getValueArray();
        for(WordStatisticsWritable item: sums){
            long[] currentValArr = item.counts.getValueArray();
            for(int i = 0; i < COUNT; i++){
                valAr[i] += currentValArr[i];
            }
        }
        return valAr;
    }
}