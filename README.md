# What's this repository?
It's the place for all of the documents or assignmensts of the `Cloud Computing` course in NCU

# Hadoop Commands
1. Compile: `hadoop com.sun.tools.javac.Main ${fileName}.java && jar cf ${jarName}.jar ${fileName}*.class`
2. Copy local to hadoop: `hadoop fs -copyFromLocal ${localFiles} ${hadoopTargetFiles}`


# lab1
## practice1: comopute the domain of given mail addresses 
* Please refer to the lab2_practice1
## practice2: Inverted Index
* Please refer to the lab2_practice2
* Input: lab2_practice2/test_data/*
* Output: lab2_practice2/result.txt
* Commands 
   1. Execution: `hadoop jar ${jarName}.jar ${fileName} /user/s107522115/lab1/practice2/input /user/s107522115/lab1/practice2/output`
   2. Show result: `hadoop fs -cat /user/s107522115/lab1/practice2/output/part-r-00000`
* ref: 
   1. http://puremonkey2010.blogspot.com/2013/11/mapreduce-inverted-index.html
   2. https://www3.nd.edu/~pbui/teaching/cse.30331.fa16/challenge11.html