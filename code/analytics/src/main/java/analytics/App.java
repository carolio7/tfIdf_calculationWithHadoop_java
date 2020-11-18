package analytics;

/*
 * @carolio7
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class App
{
    


	 
	
	public static void main( String[] args ) throws Exception
    {
		
		if (args.length != 2){
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    /*
	    job.setJarByClass(App.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    */
	    
	    
	    /*
	    //Deleting the output directory if it already exists
	    FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        */
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
    }
}
