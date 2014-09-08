import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
    /** Regex pat
     * tern to find words (alphanumeric + _). */
    final static Pattern WORD_PATTERN = Pattern.compile("(\\w+) (\\w+) ");


    /** Text object to store a word to write to output. */
    private Text word = new Text();

   /**Actual map function. Takes one document's text and emits key-value
    * pairs for each word found in the document.*/
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = WORD_PATTERN.matcher(value.toString());
        while (matcher.find()) {
            word.set(matcher.group());
            //Setting count of each word as 1
            context.write(word, new IntWritable(1));
        }
    }
}