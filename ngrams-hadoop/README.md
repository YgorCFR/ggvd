# NGrams assignment

1. For a given input file directory.
2. This code performs a ngrams processing by using hadoop.
3. The following command shows how to run the jar containg the code of the application:
```bash
java -jar NGrams.jar 2 2 input output
```
   Where:
     - The first argument is the number of n grams that user wants the code to perform.
     - The second argument is the minimum number of occurrences of a ngram case. Example: considering the occurence of a 2 ngram 'name is', this ngram occurs 3 times, if this argument set to 4 this ngram wont appear in the output.
     - The input directory.
     - The output directory.
 
