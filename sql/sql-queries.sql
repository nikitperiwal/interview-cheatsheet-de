-- Query to find number of medals for candidates who only score gold
--
--+---+-----+----+----------------+------------+-----------+
--| ID|event|YEAR|            GOLD|      SILVER|     BRONZE|
--+---+-----+----+----------------+------------+-----------+
--|  1| 100m|2016|Amthhew Mcgarray|      donald|    barbara|
--|  2| 200m|2016|         Nichole|Alvaro Eaton|janet Smith|
--|  3| 500m|2016|         Charles|     Nichole|     Susana|
--|  4| 100m|2016|          Ronald|       maria|      paula|
--|  5| 200m|2016|          Alfred|       carol|     Steven|
--|  6| 500m|2016|         Nichole|      Alfred|    Brandon|
--|  7| 100m|2016|         Charles|      Dennis|     Susana|
--|  8| 200m|2016|          Thomas|        Dawn|  catherine|
--|  9| 500m|2016|          Thomas|      Dennis|      paula|
--| 10| 100m|2016|         Charles|      Dennis|     Susana|
--| 11| 200m|2016|         jessica|      Donald|   Stefeney|
--| 12| 500m|2016|          Thomas|      Steven|  Catherine|
--+---+-----+----+----------------+------------+-----------+

--Using subquery
SELECT Gold as Player_name, count(*) as Medal_count
from events
where Gold not in (
    SELECT Silver from events
    UNION
    SELECT Bronze from events
);

--Using cte
with cte as (
    SELECT Gold as Player_name, "Gold" as Medal from event
    UNION
    SELECT Silver as Player_name, "Silver" as Medal from event
    UNION
    SELECT Bronze as Player_name, "Bronze" as Medal from event
);

SELECT Player_name, count(*) as Medal_count
from cte
group by player_name
having count(distinct Medal)>1 and max(Medal)='Gold';

-- Find the most searched filter for room and the number of occurrence; Sort in desc manner.
--+-------+-------------+------------------------+
--|user_id|date_searched|filter_room_types       |
--+-------+-------------+------------------------+
--|1      |2022-01-01   |entire home,private room|
--|2      |2022-01-02   |entire home,shared room |
--|3      |2022-01-02   |private room,shared room|
--|4      |2022-01-03   |private room            |
--+-------+-------------+------------------------+

with exploded as (
    SELECT explode(string_split("filter_room_types", "")) as Filter
    from events
)
SELECT filter, count(*) as cnt
from exploded
group by filter
order by cnt desc;

-- Return all employees with same salary in same dept
--+------+-------+------+-------+
--|emp_id|   name|salary|dept_id|
--+------+-------+------+-------+
--|   101|  sohan|  3000|     11|
--|   102|  rohan|  4000|     12|
--|   103|  mohan|  5000|     13|
--|   104|    cat|  3000|     11|
--|   105| suresh|  4000|     12|
--|   109| mahesh|  7000|     12|
--|   108|  kamal|  8000|     11|
--+------+-------+------+-------+

select a.dept_id, a.emp_id, a.name, a.salary
from employees a
inner join employees b on a.dept_id=b.dept_id
where a.salary = b.salary and a.name != b.name;
