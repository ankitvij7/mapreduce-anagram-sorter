# MapReduce Anagram Sorter
A MapReduce application that, given an input containing a single word per line, will group words that are anagrams of each other and sort the groups based on their size in descending order.

## Example :

### Input

abroad

early

natured

unrated

layer

aboard

untread

leary

relay


### Output (sorted in descending order of size)

early leary layer relay

natured unrated untread

abroad aboard

## Execution
In order to run this MR application on a multi-node cluster, you will first load the input file (input.txt) into HDFS and run this application as:

`hadoop jar mapreduce-anagram-sorter-1.0.jar AnagramSorter /input /output`
