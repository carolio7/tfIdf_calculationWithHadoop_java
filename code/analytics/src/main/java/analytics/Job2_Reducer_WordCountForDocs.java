package analytics;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2_Reducer_WordCountForDocs extends Reducer<Text, Text, Text, Text> {
	private static Text wordAtDoc = new Text();
	private static Text tf_word = new Text();

	/**
	 * ENTREE: une paire au format <documentName,["word1=n1","word2=n2",...]>
	 * 
	 * SORTIE: un ensemble de paires au format <wordk@documentName,freqWordInDoc/nbWordInDoc>
	 * exemple : <"word1@a.txt, 3/13">, <"word2@a.txt, 5/13">, ...
	 * 
	 * @param key
	 *            la clé du mapper
	 * @param values
	 *            un tableau de "word1=n1"
	 * @param context
	 *            le contexte d'application
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sumOfWordsInDocument = 0;
		HashMap<String, Integer> tempCounter = new HashMap<String, Integer>();

		// Une premiere boucle pour (1) stocker dans une structure temporaire le nombe d'occurrence de chaque mots
		// dans le document et (2) compter le nombre total de mots dans le document
		for (Text val: values) {
			String[] wordCounter = val.toString().split("=");
			// trim remove whitespaces
			tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1].trim()));
			sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1].trim());
		}
		/*
		 * result = (FileName/nbFile , word "=" termFrequency)
		 * result = (FileName/nbFile , word "=" termFrequency)
		 * tempCounter = <word , termFrequency>
		 * sumOfWordsInDocument = nbWordsInDocument
		 */
		

		// On parcourt la structure temporaire et on emet les paires
		for (String wordKey : tempCounter.keySet()) {
			wordAtDoc.set(wordKey + "@" + key.toString());
			tf_word.set(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument);
			context.write(wordAtDoc, tf_word);
		}
		/*
		 * wordAtDoc = word@FileName/nbFile
		 * tf_word = termFrequency "/" sumOfWordsInDocument
		 * Result = (word@FileName/nbFile , termFrequency "/" sumOfWordsInDocument)
		 */
				
	}

}
