/* Using it for Kafka testing: When i insert the records into this table i want that
   records to go in the deltalake employee master */
--DROP PUBLICATION avengers_pub 
--CREATE PUBLICATION avengers_pub FOR TABLE employees;
--CREATE PUBLICATION avengers_pub FOR TABLE public.employees;

SELECT * FROM pg_publication_tables WHERE pubname = 'avengers_pub';
select * from pg_settings where name='wal_level'
\du jarvis
ALTER ROLE jarvis WITH REPLICATION;
SELECT * FROM pg_replication_slots;
GRANT ALL PRIVILEGES ON DATABASE avengers TO jarvis;
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;




--------------------------------------
SELECT * FROM employees

insert into employees (emp_id,emp_name,department,salary) values
(14,'Judith','Mrketing',68000.00) ,
--(11,'Newton','Sales',71000.00) ,
(12,'Judith','Mrketing',68000.00) ,
(13,'Judith','Mrketing',69000.00)











-----------------------------------------------------

/* Problem:It should cover all the edge cases also if there are even or odd number of records.
"Given a table of students sitting in sequentially numbered seats, write a query to swap every pair of adjacent students.
If there's an odd number of students, the last one remains unchanged."

Eptd O/P:
1 Aditya to 1 Arjun
2 Arjun 2 Aditya
3 Rekha 3 Sanjay
4 Sanjay 4 Rekha
*/

select
    s.s_id,s.student,s2.s_id,s2.student
from Seats as s
join Seats as s2
on ( case when s.s_id % 2 = 0 then s.s_id -1 = s2.s_id else s.s_id + 1 = s2.s_id end )
where s.s_id != 5

/*
•Find Consecutive Rows:
Write a query to identify periods when a user was continuously active for three or more consecutive days in a user_activity table
*/

select * from user_activity



/*
•Dynamic Pivot Table:
Write a query to pivot the data from a sales table to show monthly revenue per region dynamically.
*/
sales


/*•Ranking Without RANK():
Implement a query to find the top three employees with the highest salaries in each department without using window functions.
*/





/*
1. Top 3 Customers by Transaction Volume in Last 90 Days (Optimized for Billion-Row Table)'
*/
transactions


/*
2. Remove Duplicate Records While Keeping the Latest Based on Timestamp
*/

select * from records 

insert into records (id,unique_key,value,updated_at)
values( 6 ,'A126','Value6','2025-06-01T10:00')
