import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


/**
 *
 * The base class Mapper is parameterized by
 * <in key type, in value type, out key type, out value type>.
 *
 * To support efficient serialization (conversion of data to and from
 * formats suitable for transport), Hadoop typically does not use the
 * built-in Java classes like "String" and "Long" as key or value types. The
 * wrappers Text and LongWritable implement Hadoop's serialization
 * interface (called Writable) and, unlike Java's String and Long, are
 * mutable.
 *
 */
public  class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {

    /** Text object to store a word to write to output. */
    private Text word = new Text();

   /**Actual map function. Takes one document's text and emits key-value
    * pairs for each word found in the document.*/
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        //Removing the full stops and new lines
        s = s.replaceAll("\\.", "");
        s = s.replaceAll("\\n", "");
        String sArray[] = s.split(" ");
        //Iterating for consecutive words
        for(int i = 0; i< sArray.length - 1; i++){
            context.write(new Text(sArray[i] + " " + sArray[i + 1]), new IntWritable(1));
        }
    }
}