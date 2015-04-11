import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by alpha on 2/9/15.
 */
public class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable(){
            super(DoubleWritable.class);
        }

        public double[] getValueArray() {
            Writable[] wDubValues = get();
            double[] val = new double[wDubValues.length];
            for( int i = 0; i < val.length; i++) {
                val[i] = ((DoubleWritable)wDubValues[i]).get();
            }
            return val;
        }
    }

