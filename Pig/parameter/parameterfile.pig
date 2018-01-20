

book= load '$inp' using PigStorage() as (line:chararray);
dump book;
store book into '$output';

-- pass file path as parameter 
--for local mode -f current file
--pig -x local -p inp=/home/hduser/wordCountText -f parameterfile.pig

--for hdfs mode
--pig -p inp=/niit/wordCountText -f parameterfile.pig

