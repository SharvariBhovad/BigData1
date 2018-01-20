book1= load '/home/hduser/sample.txt' 
using PigStorage(',') 
AS (name:chararray, city:chararray, age:long);

book2= load '/home/hduser/sample2.txt' 
using PigStorage(',') 
AS (name:chararray, city:chararray, age:long);

bookcombined= union book1,book2;  --combining above two book

split bookcombined into book3 if city == 'mumbai', book4 if city == 'goa', book5 if age == 22, otherbag OTHERWISE;

dump otherbag;

/* here, if condition is satisfied than row in which condition returns true will be copied to respective bag
if a row satisfies all the 3 condition then it will go in all three bag ---- its not perfect split
*/
 
