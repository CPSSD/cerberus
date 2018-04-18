# Sprint 7 Demo Payload

The sprint 7 demo involves processing a the google ngrams data for 2-grams.
The map reduce produces a count of how many unique words precede each
word word in the dataset. It only takes into account data from years
past 2000.

### Map

The map phase processes lines in a tab seperated csv format.
For each pair of ngrams, the first word is outputed as the value for the second word.

### Parition 

We use a custom partitioning function to alphabetize the output.

### Reduce

The reduce phase counts the number of unique values for each key and outputs 
this count.
