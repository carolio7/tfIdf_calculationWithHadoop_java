# TF-IDF calculation 

## 1. Subject presentation
In this project, we will use hadoop for building a application which compute TF-IDF caluclation of all words in a collection of some documents.
You can find [here](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) about TF-IDF.

It used for information retrieval.

In our analysis, stop words are not considered. The file (stopwords_en.txt] contain all stop words in english.



## 2. Preliminary:
Hadoop version: 2.10.0

Start dfs: $HADOOP_HOME/sbin/start-dfs.sh
Start yarn: $HADOOP_HOME/sbin/start-yarn.sh



## 3. Programm:

1- Job_1 calculate the frequency of term in each document, it returns: (word@FileName/nbFile, termFrequency).
2- Job_2 will add number total of word in each document : (word@FileName/nbFile , termFrequency "/" sumOfWordsInDocument)
    Map Job 2: 
                output = (FileName/nbFile , word "=" termFrequency)

    Reduce Job 2:
                input = (FileName/nbFile , word "=" termFrequency)
                tempCounter = <word , termFrequency>
                sumOfWordsInDocument = nbWordsInDocument
                wordAtDoc = word@FileName/nbFile
                tf_word = termFrequency "/" sumOfWordsInDocument
                Result = (word@FileName/nbFile , termFrequency "/" sumOfWordsInDocument)
    
3 - Job_3 calculate Term_Frequency * IDF : wordFileName [TF*idf]


## 4.Command for launching the application: 
hadoop jar tf_idfCalculation.jar analytics.App /input/texte /output

