package analytics;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2_Mapper_WordCountForDocs extends Mapper<Object, Text, Text, Text> {
	
	// Reuse writables
		private static Text docName = new Text();
		private static Text wordCount = new Text();
		
		/**
		 * SORTIE:  un ensemble de paires du type <documentName, mot=freq>
		 * 
		 * @param key
		 *            est l'offset de la ligne du fichier considéré;
		 * @param value
		 *            est la ligne pointée par le fichier au format mot@documentName \t freq
		 *            Exemple: adaptability@callwild/2	1
		 * @param context 
		 * 			est le contexte d'application
		 */
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
					
		String[] wordDocCount = value.toString().split("\t");
		String[] wordAndDoc = wordDocCount[0].split("@");
		docName.set(wordAndDoc[1]);
		wordCount.set(wordAndDoc[0] + " = " + wordDocCount[1]);
		context.write(docName, wordCount);  // (callwild/2, adaptability = 1)
		
		/*
		 * wordDocCount = [word@FileName/nbFile , termFrequency]
		 * wordAndDoc = [word , FileName/nbFile]
		 * docName = FileName/nbFile
		 * wordCount = word "=" termFrequency
		 * result = (FileName/nbFile , word "=" termFrequency)
		 * 
		 */
		        
	}

}
