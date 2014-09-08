import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Reducer for word count.
 *
 * Like the Mapper base class, the base class Reducer is parameterized by
 * <in key type, in value type, out key type, out value type>.
 *
 * For each Text key, which represents a word, this reducer gets a list of
 * LongWritable values, computes the sum of those values, and the key-value
 * pair (word, sum).
 */
public  class SumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Actual reduce function.
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        //Calculates the sum and writes in the context.
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}