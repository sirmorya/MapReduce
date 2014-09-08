import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Driver program to run the map Reduce model to calculate the single word frequency.
 */
public class WordCount {

    /** Entry-point for our program. Constructs a Job object representing a single
     * Map-Reduce job and asks Hadoop to run it. When running on a cluster, the
     * final "waitForCompletion" call will distribute the code for this job across
     * the cluster.
     */
    public static void main(String[] args) throws Exception {

        //Instantiating Job
        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("Word Count");
        //Setting the input and output file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Declaring the Mapper and the Reducer tasks
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(SumReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Exit the system
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
 }
