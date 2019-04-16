






/*

--======= 30 =======
--This is to practive view
--ALTER TABLE address DROP COLUMN address2;
create view my_view as 
select first_name, last_name, /*cu.address_id, ad.address_id,*/ address from customer cu, address ad
where cu.address_id = ad.address_id; 

alter view my_view rename to views_; 
select * from views_;
drop view if exists views_;


--======= 29 =====
--This is an assessment
DROP TABLE if exists students;
create table students (student_id serial primary key, first_name varchar(50) not null, 
							  last_name varchar(50) not null, homeroom_num integer not null,
							  phone varchar(10) not null unique, email varchar(50) unique,
							  grad_year integer check(grad_year>2000));
INSERT INTO students(first_name, last_name, phone, grad_year, homeroom_num) values('David', 'Will',
	'7775551243', 2003, 5);
SELECT * FROM students;																				  
																				 
																				 
--========== 28 ======
--This is to test UNIQUE CONSTRAINT
DROP TABLE if exists test_1;
CREATE TABLE test_1 (name varchar(30), age integer unique); --null is unique, so different nulls are different from each other.
INSERT INTO test_1 (name, age) values('z', 3), ('z', 30);  --age must be different from other values.
select * from test_1;


--======= 27 ======
--This is to practice check(boolean expression) clause
DROP TABLE IF EXISTS test_1;
CREATE TABLE test_1 (
	stu_name varchar(50) not null CHECK (char_length(stu_name) < 10),
	signup_time timestamp check(signup_time > '2000-01-01'), 
	val integer check(val>0)
);
INSERT INTO test_1 (stu_name) values ('std'), ('ssdsdsdsd'); --fewer than 10 characters
INSERT INTO test_1 (stu_name, signup_time) values ('sd', '10990-01-01'); --the time must be checked.
INSERT INTO test_1 (stu_name, val) values('sddf', 3); --val must bed checked.
ALTER TABLE test_1 add column score integer constraint positive_score check(score >0); --name the constraint for readability
INSERT into test_1 (stu_name, score) values('sdfs', -3); --
SELECT * FROM test_1;


--========= 26 ======
--This is to practice dropping tables
drop table if exists users_copy;
create table users_copy (like users1);
insert into users_copy select * from users1;
select * from users_copy;


--====== 25 ======
--This is to practice alter table clauses
--ALTER TABLE users add COLUMN test_1 integer;
--alter table users drop column test_1;
--alter table users rename column test_1 to test;
--alter table users rename to users1;
select * from users1;


--====== 24 =======
--This is to practice delete clause
SELECT * FROM users;
DELETE FROM users_copy where name = 'j' returning *;
delete from users_copy returning *;
insert into users (name, age) values('a', 2), ('b', 3);
update users set user_id = 100 where name = 'b';
insert into users(name, age) values('c', 3);
insert into users(user_id, name, age) values(23, 'c', 3);
insert into users(name, age) values('dd', 7); --tests showed that serial type keeps track of 
--what the last number is.


--====== 23 =======
--This is to practice update clause
UPDATE users_copy SET email = '....';
update users_copy set email ='update email for ' || name;
update users_copy set age = user_id returning age;
--SELECT * FROM users_copy;


--======= 22 =======
--This is to practice inserting into tables
CREATE TABLE users (
	user_id serial PRIMARY KEY,
	name varchar(200) not null,
	age integer not null
)

SELECT * FROM users;
INSERT INTO users (name, age)
values ('zhao', 22),
		('dong', 23);
		
INSERT INTO users (name, age)
values ('sd', 23), ('j', 3);
select * from USERS;

CREATE TABLE users_copy (like users, email varchar(200));

INSERT INTO users_copy select * from users where name = 'j';

INSERT INTO users_copy(user_id, name, age, email)
values(100, 'test', 23, 'a@osu.edu'); --lost function of serial?
SELECT * from users_copy;


--======= 21 =======
--This is to practice creating tables
CREATE TABLE account (
	user_id serial PRIMARY KEY,
	user_name varchar(50) UNIQUE NOT NULL,
	passphrase varchar (50) NOT NULL,
	email varchar(200) UNIQUE NOT NULL,
	created_on timestamp not null,
	last_login timestamp not null
);

CREATE TABLE role (
	role_id serial PRIMARY KEY,
	role_name varchar(500) UNIQUE NOT NULL
);


--====== 20 ======
--This is to practice the exercises
SELECT * FROM cd.facilities;

SELECT name, membercost FROM cd.facilities;

SELECT name, membercost FROM cd.facilities WHERE membercost >0;

SELECT facid, name, membercost, monthlymaintenance FROM cd.facilities
WHERE membercost>0 AND membercost < monthlymaintenance/50;

SELECT name FROM cd.facilities
WHERE name LIKE '%Tennis%';

SELECT * FROM cd.facilities
WHERE facid IN (1, 5);

SELECT * FROM cd.members;
SELECT memid, surname, firstname, joindate FROM cd.members
WHERE joindate >= '2012-09-01'
ORDER BY joindate;

SELECT DISTINCT(surname) FROM cd.members
ORDER BY surname
LIMIT 10;

SELECT joindate FROM cd.members
ORDER BY joindate DESC
LIMIT 1;

SELECT max(joindate) FROM cd.members;

SELECT count(*) FROM cd.facilities
WHERE guestcost >= 10;

SELECT facid, slots FROM cd.bookings
WHERE starttime BETWEEN '2012-09-01' AND '2012-09-30';

SELECT facid, SUM(slots) total_slots FROM cd.bookings
GROUP BY facid
HAVING SUM(slots) > 1000
ORDER BY facid;

SELECT starttime, name FROM cd.bookings book
LEFT JOIN cd.facilities fac ON book.facid = fac.facid
WHERE
book.facid IN (SELECT facid FROM cd.facilities WHERE name LIKE 'Tennis Court%')
AND starttime BETWEEN '2012-09-21 00:00:00' AND '2012-09-22 00:00:00'
ORDER BY starttime;


SELECT starttime FROM cd.bookings
WHERE memid =
(SELECT memid FROM cd.members
WHERE firstname || surname = 'DavidFarrell');

SELECT starttime FROM cd.bookings book, cd.members memb 
WHERE book.memid = memb.memid AND memb.surname='Farrell' AND
memb.firstname='David'
ORDER BY starttime;


--====== 19 =======
--This is to practice self join
SELECT a.first_name, a.last_name, b.first_name, b.last_name 
FROM customer a, customer b --self-join
WHERE a.first_name = b.last_name
ORDER BY a.first_name;

SELECT c.first_name, c.last_name, d.first_name test, d.last_name
FROM customer c
LEFT /*INNER*/ JOIN customer d ON c.first_name = d.last_name;


--======= 18 ========
--This is to practice subquery
SELECT film_id, title FROM film
WHERE
film_id IN 
(SELECT inventory.film_id
FROM rental
INNER JOIN inventory ON inventory.inventory_id = rental.inventory_id
WHERE 
return_date BETWEEN '2005-05-29' AND '2005-05-30');

--SELECT inventory_id FROM inventory
--ORDER BY inventory_id;



--======= 17 ========
--This is to practice string operations
SELECT upper(first_name), lower(last_name), first_name || ' ' || last_name full_name, char_length(last_name || ' ' || first_name) len FROM staff limit 3;


--========= 16 ========
--This is to practice mathematical functions
SELECT max(amount) FROM payment
GROUP BY amount;

SELECT DISTINCT(amount) am FROM payment
ORDER BY am;

SELECT customer_id, staff_id, customer_id/staff_id total FROM payment limit 3;

SELECT rental_date, return_date, extract(day from return_date - rental_date) FROM payment, rental limit 3;


--========= 15 ======
--This is to extract time information from time stamp
SELECT title, last_update tm, 
extract(century from last_update) centry,
extract(second from last_update) ext 

FROM film limit 10;


--====== 14 ======
--This is to practice 
SELECT * FROM film limit 3;
SELECT * FROM actor limit 3;
SELECT f.film_id num, f.title str FROM film f
UNION ALL --UNION (remove the duplicated ones), UNION ALL (keep all duplicated ones, like concatenate)
SELECT a.actor_id num, a.first_name str FROM actor a
ORDER BY num, str DESC;


--======= 13 ====
SELECT cust.customer_id, f.film_id  FROM customer cust
FULL JOIN film_actor f ON cust.customer_id = f.film_id
WHERE cust.customer_id IS null OR f.film_id IS null;


--======= 12 ========
--This is to practice outer join
--SELECT * FROM film  limit 3;
--SELECT * FROM inventory limit 3;
SELECT film.film_id, film.title, inventory_id FROM film
LEFT /*OUTER*/ JOIN inventory AS invt ON film.film_id = invt.film_id
WHERE invt.inventory_id IS null;

SELECT f.film_id, title, inventory_id FROM film f
FULL JOIN inventory invt ON f.film_id = invt.film_id
WHERE invt.inventory_id IS null
ORDER BY f.film_id;


--======= 11 ======
SELECT /*title,*/ /*release_year,*/ count(name) FROM film
/*INNER*/ JOIN language /*AS*/ lan ON film.language_id = lan.language_id
WHERE release_year IN (2006, 2007)
GROUP BY name;


--======= 10 ======
--This is to practice INNER JOIN
SELECT customer.customer_id AS cust, first_name, last_name, amount, payment_date FROM customer
INNER JOIN payment ON customer.customer_id = payment.customer_id --can not use cust
ORDER BY first_name, amount DESC; --specify the table if the column names are the same.
--for the same person, order the transactions in descending order.


--======= 9 =======
--This is to practice AS
SELECT customer_id AS customer, SUM(amount) AS total FROM payment
GROUP BY customer
ORDER BY total DESC;


--====== 8 =====
--This is to practice GROUP BY, HAVING and all other skills learnt
SELECT customer_id, SUM(amount) FROM payment
WHERE staff_id = 2 --filter out the other staffs
GROUP BY customer_id
HAVING SUM(amount)>=110; --only keep the desired amount

SELECT COUNT(*) FROM film
WHERE title LIKE 'J%';

SELECT * FROM customer
WHERE (first_name LIKE 'E%' OR last_name LIKE 'E%') AND address_id < 500
ORDER BY customer_id DESC;


--====== 7 ======
--This is to practice LIMIT and ORDER BY
SELECT * FROM customer ORDER BY store_id DESC, address_id LIMIT 2; --LIMIT must be at the end?


--======== 6 =======
--This is to practice count()
SELECT count(*) FROM city;
SELECT count(*) FROM city WHERE country_id > 100;
SELECT count(city) FROM city WHERE country_id <= 100;


--====== 5 =======
--This is to practice WHERE
SELECT district, city_id FROM address WHERE address = '692 Joliet Street';
SELECT staff_id FROM  payment WHERE customer_id = 341 AND amount > 6;
--SELECT * FROM rental;
SELECT rental_date FROM rental WHERE (rental_id > 3 AND rental_id <7) AND (inventory_id > 1700 AND inventory_id < 3000);


--====== 4 =======
--This is to practice DISTINCT
SELECT * FROM film;
SELECT DISTINCT release_year FROM film; --only return the distinct value
SELECT language_id,rental_duration, rental_rate FROM film; --only return the distinct combination
--So these combinations at least have one different column.

--======= 3 ========
--This is to explore table fim_actor
SELECT * FROM film_actor; --don't forget to end with ';'

--========= 2 =========
/*This is to explore table actor*/
SELECT * FROM actor; --must end with semicolon
SELECT first_name FROM actor;

--========= 1 =========
/*This is to explore customer*/
SELECT * FROM customer; --comment
SELECT first_name, last_name, email FROM customer;

*/