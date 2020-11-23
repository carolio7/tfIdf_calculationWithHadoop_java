package analytics;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3_Mapper_TF_IDF extends Mapper<LongWritable, Text, Text, Text> {
	
	// Reuse writables
	private static Text cle_out_word = new Text();
	private static Text valeur_out_docAndCounters = new Text();

	/**
	 *
	 * 
	 * SORTIE : une paire du type <word,documentName=freqWordIndoc/nBWordInDoc 
	 * Exemple : <toto, book.txt=3/1500>
	 * 
	 * @param key
	 *            est l'offeset de la ligne considérée dans le fichier;
	 * @param value
	 *            est la ligne du fichier pointée par l'offeset
	 *			  Son format est wordk@documentName \t freqWordInDoc/nbWordInDoc
	 *			  Exemple :	toto@book.txt \t 3/1500
	 * @param context
	 *            le contexte du job
	 * 
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] wordAndCounters = value.toString().split("\t");
		String[] wordAndDoc = wordAndCounters[0].split("@");
		cle_out_word.set(new Text(wordAndDoc[0]));
		valeur_out_docAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
		context.write(cle_out_word, valeur_out_docAndCounters);

	}
	
}
