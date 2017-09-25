Note:
I have executed the programs in Eclipse IDE. So, the execution instructions provided below are applicable for Eclipse. We can use the same arguments to execute the programs in Hadoop


Instructions

PageRank.java

1. Compile both PageRank.java and Rank.java(Used to store the Title and Rank for sorting later)
2. Execute the PageRank.java to claculate the PageRank ( created the pagerank.jar from the Eclipse project)
2. Pass the input directory in args[0].
3. Pass the output directory in args[1]. 
4. For example, /home/cloudera/input /home/cloudera/output
5. Verify the output present in the directory args[1]. In this case, /home/cloudera/output. This file contains all the records, we can extract top 100 records by providing the following command head -100 oldfile > new file. 


