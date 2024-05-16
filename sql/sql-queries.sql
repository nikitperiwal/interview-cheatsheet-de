
-- Find duplicates in a table
SELECT emp_id, count(*)
FROM employees
GROUP BY emp_id
HAVING count(*)>1;

-- Delete duplicate records from table
with CTE as (
    SELECT *, row_number() over(partition by emp_id order by emp_id asc) as row_number from employees
)
delete from CTE where rn>1;

-- Employees not present in dept table
Select emp_id, name
from employees e
left join department d on e.dept_id = d.dept_id
where d.dept_name is null;

-- Second highest salary in each dept
with CTE as (
    SELECT dept_id, emp_id, name, salary, dense_rank() over(partition by dept_id order by salary desc) as rnk
    FROM employees
)
Select dept_id, emp_id, name, salary
from CTE
where rnk = 2;

-- Find all transactions done by shilpa
Select * from employees where lower(name)='shilpa';

-- Give list of all employees where emp salary > manager salary
Select e.emp_id, e.salary, e.manager_id, m.salary
from employees e
inner join employees m on e.manager_id = m.emp_id
where e.salary > m.salary;

-- Swap gender values
update employees set gender = (
    case
        when gender="Male" then "Female"
        when gender="Female" then "Male"
        else "Others"
    end
)

-- In a sales table with columns id, sale_date, employee_id, and amount,
-- write a query to display the total sales amount by each employee for the current month,
-- ensuring only those with a total sales amount greater than $10,000 are listed. (Visa, Data Engineer)
SELECT employee_id, sum(amount) as total_sales
from sales
where date_trunc("month", sale_date) = date_trunc("month", CURRENT_DATE)
group by employee_id
having sum(amount)>10000;

-- Given two tables, employees (columns: id,name, department_id) and departments(columns: id, department_name),
-- write a SQL query to find all employees and their department names, including those without a department.
-- (Associate Data Engineer, Maersk)
Select e.id, e.name, e.dept_id, e.dept_name
from employees e
left join department d on e.dept_id = d.id;

-- In a sales table with columns product_id, sale_date, and quantity_sold,
-- write a query to rank products by the quantity sold on a specific date, showing the product ID and its rank.
-- (Data Engineer, 1st Technical Round (SQL + Python), Confluent)
Select *, row_number() over(order by quantity_sold desc) as product_ranking
    from products
    where sales_date = "2023-02-02"

-- Given a table salary_records with columns employee_id, salary_date, and salary,
-- write a query to show the salary growth for each employee by comparing the current salary after each promotion.
-- Add a new column, which tells the promotion number of the employee in string format ‘promotion number : 1’, if the
-- record indicates the first salary of employee, name it as ‘Joining Salary’. (Flipkart, Advanced SQL & Data Modelling)
with salary as (
    Select employee_id, salary,
     (salary - lag(salary, 1) over(partition by employee_id order by salary_date asc)) as prev_salary_diff,
     row_number() over(partition by employee_id order by salary_date asc) as  rn
    from salary_records
)
Select employee_id, salary, prev_salary_diff
    case
        when prev_salary_diff is null then 'Joining Salary'
        else concat("promotion number : ", (rn-1))
    end
from salary

-- How to use row/range between:
-- rows between Unbounded Preceding and Unbounded Following
-- rows between 3 Preceding and 1 Preceding
-- rows between 1 Following and 3 Following
-- range between 2 Preceding and Current Row


-- In a customer_orders table with customer_id, order_date, and order_value,
-- write a query to display a running total of order values for each customer over time.
Select customer_id, order_date, order_value,
    sum(order_value) over(partition by customer_id order by order_date asc rows between unbounded preceding and current row) as running_total
from customer_orders;

-- Given a table project_tasks with columns task_id, project_id, start_date, and end_date,
-- write a SQL query to find the next task each task will start after within the same project.
-- Assume no tasks within the same project start on the same day. (McKinsey & Company, Senior Data Engineer)
Select project_id, task_id, start_date,
    lead(start_date, 1) over(partition by project_id order by start_date asc) as next_start_date
From project_tasks

-- In an employee_reviews table with employee_id, review_date, and performance_score,
-- write a SQL query to compare each employee's current performance score with their average performance score
-- from all previous reviews.
Select employee_id, review_date, performance_score,
    avg(performance_score) over(partition by employee_id order by review_date asc rows between unbounded preceding and 1 preceding) as avg_prev_score
from employee_reviews

-- In a customer_logins table with customer_id and login_date,
-- write a SQL query to find customers who logged in consecutively for at least 3 days. (LinkedIn, Senior Data Engineer)
With login_history as (
    Select customer_id, login_date,
        lag(login_date, 2) over(partition by customer_id order by login_date asc) as second_last_login,
        (login_date - lag(login_date, 2) over(partition by customer_id order by login_date asc)) as date_diff
    from customer_logins
    where second_last_login is not null and date_diff = 2;
)

select customer_id, login_date, second_last_login
from login_history
group by customer_id;

-- Table product_sales records daily sales for products, with columns - sale_date, product_id, amount.
-- The goal is to analyze the sales trend by product, calculating a 3-day moving average of sales, including the current
-- day, the day before, and the day after. (Oracle, Data Engineer)
Select project_id, sale_date
    avg(amount) over(partition by project_id order by sales_date rows between 1 preceding and 1 following) as moving_avg
from product_sales

-- Table location_temps logs daily temperatures for locations, with columns record_date, location_id, and temperature.
-- The goal is to analyze temperature trends by location, calculating the average temperature for each day considering
-- records within a 2-degree temperature range of the current day's temperature. (Oracle, Data Engineer)
Select location_id, record_date
    avg(amt) over(partition by location_id order by record_date range between 2 preceding and 2 following) as avg_temp
from location_temps

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
    SELECT explode(string_split("filter_room_types", ",")) as Filter
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

