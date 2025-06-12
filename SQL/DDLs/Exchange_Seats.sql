CREATE TABLE Seats (
    s_id INT PRIMARY KEY,
    student VARCHAR(50)
);

--  INSERT Script


INSERT INTO Seats (s_id, student) VALUES
(5, 'Aravind'),
(1, 'Aditya'),
(2, 'Arjun'),
(3, 'Rekha'),
(4, 'Sanjay');


------------------------

CREATE TABLE user_activity (
    user_id INT,
    activity_date DATE
);

INSERT INTO user_activity (user_id, activity_date) VALUES
(1, '2024-01-01'),
(1, '2024-01-02'),
(1, '2024-01-03'),
(1, '2024-01-05'),
(2, '2024-01-10'),
(2, '2024-01-11'),
(2, '2024-01-13'),
(3, '2024-01-01'),
(3, '2024-01-02'),
(3, '2024-01-03'),
(3, '2024-01-04');

------------------------

🔹 2. Dynamic Pivot Table (Sales Revenue)
Note: Most RDBMS (e.g., MySQL, PostgreSQL) do not support true dynamic SQL directly in a query.
You typically need to write stored procedures or dynamic SQL in application code. But here's a simplified structure.


CREATE TABLE sales (
    id INT PRIMARY KEY,
    region VARCHAR(50),
    sale_date DATE,
    revenue DECIMAL(10, 2)
);


INSERT INTO sales (id, region, sale_date, revenue) VALUES
(1, 'North', '2024-01-15', 1000),
(2, 'South', '2024-01-20', 1500),
(3, 'North', '2024-02-10', 2000),
(4, 'South', '2024-02-17', 1800),
(5, 'East', '2024-01-12', 1200),
(6, 'East', '2024-02-05', 1400);

--------------------

3. Ranking Without RANK() (Top 3 Salaries Per Department)

CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    department VARCHAR(50),
    salary DECIMAL(10, 2)
);

INSERT INTO employees (emp_id, emp_name, department, salary) VALUES
(1, 'Alice', 'HR', 70000),
(2, 'Bob', 'HR', 80000),
(3, 'Carol', 'HR', 60000),
(4, 'David', 'IT', 90000),
(5, 'Eve', 'IT', 85000),
(6, 'Frank', 'IT', 95000),
(7, 'Grace', 'Sales', 75000),
(8, 'Heidi', 'Sales', 72000),
(9, 'Ivan', 'Sales', 71000),
(10, 'Judy', 'Sales', 68000);

--------------------


✅ 1. Top 3 Customers by Transaction Volume in Last 90 Days (Optimized for Billion-Row Table)'

CREATE TABLE transactions (
    transaction_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    amount DECIMAL(12, 2),
    transaction_date DATE
);

INSERT INTO transactions (transaction_id, customer_id, amount, transaction_date) VALUES
(1, 101, 500.00, CURRENT_DATE - INTERVAL '10 DAY'),
(2, 102, 1500.00, CURRENT_DATE - INTERVAL '20 DAY'),
(3, 101, 800.00, CURRENT_DATE - INTERVAL '30 DAY'),
(4, 103, 300.00, CURRENT_DATE - INTERVAL '40 DAY'),
(5, 102, 2000.00, CURRENT_DATE - INTERVAL '50 DAY'),
(6, 101, 100.00, CURRENT_DATE - INTERVAL '100 DAY');


2. Remove Duplicate Records While Keeping the Latest Based on Timestamp

CREATE TABLE records (
    id SERIAL PRIMARY KEY,
    unique_key VARCHAR(100),
    value TEXT,
    updated_at TIMESTAMP
);


INSERT INTO records (unique_key, value, updated_at) VALUES
('A123', 'Value1', '2024-01-01 10:00:00'),
('A123', 'Value2', '2024-01-01 12:00:00'),
('B456', 'Value3', '2024-02-01 09:00:00'),
('B456', 'Value4', '2024-02-01 11:00:00'),
('C789', 'Value5', '2024-03-01 08:00:00');
