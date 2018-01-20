bag1= load '/home/hduser/group1file.txt'
	using PigStorage(',')
	as (prod:int,pqty:int);

bag2= load '/home/hduser/group2file.txt'
	using PigStorage(',')
	as (prod:int,sqty:int);

--dump bag2;

bagnew= cogroup bag1 by $0, bag2 by $0;
--dump bagnew;

/*
(101,{(101,30),(101,20)},{(101,40),(101,30)})
(102,{(102,40),(102,25)},{(102,50),(102,30)})
*/

sumbag= foreach bagnew GENERATE group, SUM(bag1.pqty),SUM(bag2.sqty);
--dump sumbag;

/*
(101,50,70)
(102,65,80)
*/

sumbag1= foreach bagnew GENERATE group, SUM(bag1.pqty),COUNT(bag1),SUM(bag2.sqty),COUNT(bag2);
dump sumbag1;
/*
(101,50,2,70,2)
(102,65,2,80,2)
*/
