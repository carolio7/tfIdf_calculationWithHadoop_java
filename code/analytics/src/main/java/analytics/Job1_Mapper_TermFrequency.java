package analytics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Job1_Mapper_TermFrequency extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
    private Text word_et_doc = new Text();

	//final String fichier_stpwd = "monProgramme/stopwords_en.txt";
    List<String> stopwords = new ArrayList<String>();
	
	@Override
	public void setup(Context context) throws IOException{
        stopwords = Files.readAllLines(Paths.get("stopwords_en.txt"));

	}

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	// récuperation du nom de fichier
    	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    	
    	// récupération du nombre de fichier dans le corpus
        Configuration conf = context.getConfiguration();
        String countFile = conf.get("nbInputFile");
        
        // Convertir tous les mot en minuscule
    	String line = value.toString().toLowerCase();
        
        line = line.replaceAll("[,?;.:!§*'(){}|`=+-_&~@\"]", " ");

        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
        	String mot = tokenizer.nextToken();
        	
        	if ((mot.length() > 2) && (!(stopwords.contains(mot)))) {
        		word_et_doc.set(mot + "@" + fileName + "/" + countFile );
        		context.write(word_et_doc, one);
        	}
            
        }
    }
    

    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        cleanup(context);
    }

}
