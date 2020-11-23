package analytics;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3_Reducer_TF_IDF extends Reducer<Text, Text, Text, Text> {
	
	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private static Text OUT_CLE = new Text();
	private static Text OUT_VALEUR = new Text();

	/**
	 * ENTREE: une paire du type  <word, ["doc1=n1/N1", "doc2=n2/N2"]>
	 * 
	 * SORTIE: une paire de la forme <word@fileName,  [d/D, n/N, TF-IDF]> avec :
	 *				- d est le nombre de documents qui contient word
	 *				- D est le nombre total de Documents
	 *				- n est le nombre d'occurrences de word dans fileName
	 *				- N est le nombre de mots dans fileName
	 *				- TF-IDF est le tf-idf de wordname
	 * @param key
	 *            est la clé retournée par le mapper
	 * @param values
	 *            est  un tableau de la forme ["doc1=n1/N1", "doc2=n2/N2", ...]
	 * @param context
	 *            contient le contexte d'exécution du job
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		// Nombre total d'occurences du mot et comptabilisation du nombre de documents avec le mot en question 
		int numberOfDocsInCorpusWithKey = 0;
		Map<String, String> tempFrequencies = new HashMap<String, String>(); // Stockera des éléments du type "doc1" => "n1/N1"
		
		for (Text val : values) {
			String[] documentAndFrequencies = val.toString().split("=");
			if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
				numberOfDocsInCorpusWithKey++;
			}
			tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
		}

		
		// Cette deuxième boucle pour effectuer le calcul sur chacune de nos éléments stockés dans tempFrequencie
		for (String doc: tempFrequencies.keySet()) {
			// Recupération du nombre total de document dans le corpus
			String[] document = doc.split("/");
			Double nbTotalDoc = Double.valueOf(document[1]);
			String[] wordFrequenceAndTotalWords = tempFrequencies.get(doc).split("/");
			
			// calcul du tf
			double tf ;
			tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
					/ Double.valueOf(wordFrequenceAndTotalWords[1]));
			
			// Calcul de l'idf
			double idf ;
			idf = Math.log10( nbTotalDoc / 
					Double.valueOf(numberOfDocsInCorpusWithKey));
			//idf = DF.format(idf);

			// calcul du tf-idf
			double tfIdf ;
			tfIdf = tf * idf;

			// Création de la sortie selon le format spécifié

			OUT_CLE.set(key + "@" + document[0]);
			OUT_VALEUR.set("[ " + DF.format(tfIdf) + " ]");
			
			context.write(OUT_CLE, OUT_VALEUR);
		}
	}
	
	
}
