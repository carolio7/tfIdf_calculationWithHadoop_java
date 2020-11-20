package analytics;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
	    FileSystem fs = FileSystem.get(conf);
	    
	    /*
	     * Upload stopwords file from local to HDFS
	     * stopwords FILE have to be in HDFS before sending it as Distributed Cache
	     * You can use lines below for copying stopwords file from Local to HDFS
	    Path stopwordPath_dfs = new Path("/stopword_nel.txt");
        OutputStream os = fs.create(stopwordPath_dfs);
        InputStream is = new BufferedInputStream(new FileInputStream("../../../../../stopwords_en.txt"));
        //Data set is getting copied into input stream through buffer mechanism
        IOUtils.copyBytes(is, os, conf); // copy stream is in os
        */
	    
	    
	    // Counting number of file in the input
        int count = 0;
        boolean recursive = false;
        RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(args[0]), recursive);
        while (ri.hasNext()){
            count++;
            ri.next();
        }
        conf.set("nbInputFile", Integer.toString(count));
        // Printing result of counting
        System.out.println("The input directory contains : " + count + " documents");
		
        
        // job1 : compter la fréquence des termes dans chaque document
        Job job1 = new Job(conf, "Term Frequency"); 
        // On precise les classes MyProgram, Map et Reduce
        job1.setJarByClass(App.class);
        job1.setMapperClass(Job1_Mapper_TermFrequency.class);
        job1.setReducerClass(Job1_Reducer_TermFrequency.class);

        // Definition des types clé/valeur à la sortie du job
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // envoi du fichier stopwords dans les datanodes (clusters)
        job1.addCacheFile(new URI("/stopwords_en.txt"));

        Path inputFilePath1 = new Path(args[0]);
        Path outputFilePath1 = new Path(args[1] + "/job1");
        // On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job1, true);
        FileInputFormat.addInputPath(job1, inputFilePath1);
        FileOutputFormat.setOutputPath(job1, outputFilePath1);
        if (fs.exists(outputFilePath1)) {
            fs.delete(outputFilePath1, true);
        }
        job1.waitForCompletion(true);
        
	    
		
    }
}
