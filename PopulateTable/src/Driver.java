import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*TODO
 * Implement time zone conversion for time stamps (waiting on Joe's team)
 * Write automated script for automatic updates (to run on Google cloud machine)
 * */
public class Driver {
	public static void main (String [] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		/*Split input by line count to ensure each node gets an equal share of data*/
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 278810);	    
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
	    job.setJarByClass(Driver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(map1.class);
	    job.setReducerClass(reducer1.class);
	    job.setNumReduceTasks(1);
	    job.setInputFormatClass(NLineInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);  
//	    job.setNumReduceTasks(1);
//	    FileInputFormat.setInputDirRecursive(job, true);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("/finalOut"));   
	    job.waitForCompletion(true);
		
	}
	
}
