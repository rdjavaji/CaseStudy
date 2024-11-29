--Create a customers table: 
CREATE TABLE customers (
customer_id INT PRIMARY KEY,
first_name VARCHAR(50),
last_name VARCHAR(50),
address VARCHAR(100),
city VARCHAR(50),
state VARCHAR(50),
zip VARCHAR(20)
);

--Create a accounts table:
CREATE TABLE accounts (
account_id INT PRIMARY KEY,
customer_id INT,
account_type VARCHAR(50),
balance DECIMAL(10, 2),
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

--Create a transactions table:
CREATE TABLE transactions (
transaction_id INT PRIMARY KEY,
account_id INT,
transaction_date DATE,
transaction_amount DECIMAL(10, 2),
transaction_type VARCHAR(50),
FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

--Create a loans table:
CREATE TABLE loans (
loan_id INT PRIMARY KEY,
customer_id INT,
loan_amount DECIMAL(10, 2),
interest_rate DECIMAL(5, 2),
loan_term INT,
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

--Create a loan_payments table:
CREATE TABLE loan_payments (
payment_id INT PRIMARY KEY,
loan_id INT,
payment_date DATE,
payment_amount DECIMAL(10, 2),
FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
);

-- Insert 10 Values into customers table
INSERT INTO customers (customer_id, first_name, last_name, address, city, state,
zip) VALUES
(1, 'John', 'Doe', '123 Elm St', 'Springfield', 'IL', '62701'),
(2, 'Jane', 'Smith', '456 Oak St', 'Chicago', 'IL', '60614'),
(3, 'Emily', 'Johnson', '789 Pine St', 'Dallas', 'TX', '75201'),
(4, 'Michael', 'Williams', '101 Maple St', 'Seattle', 'WA', '98101'),
(5, 'Sarah', 'Brown', '202 Birch St', 'New York', 'NY', '10001'),
(6, 'David', 'Jones', '303 Cedar St', 'Los Angeles', 'CA', '90001'),
(7, 'Laura', 'Garcia', '404 Willow St', 'San Francisco', 'CA', '94101'),
(8, 'James', 'Martinez', '505 Redwood St', 'Houston', 'TX', '77001'),
(9, 'Olivia', 'Davis', '606 Fir St', 'Boston', 'MA', '02101'),
(10, 'Daniel', 'Rodriguez', '707 Spruce St', 'Philadelphia', 'PA', '19101')
(11, 'Andy', 'Josheny', '123 yulm St', 'Chicago', 'IL', '627098'),
(12, 'Eva', 'antony', '153 balliol St', 'Chicago', 'PL', '667098');

-- Insert 10 Values into accounts table
INSERT INTO accounts (account_id, customer_id, account_type, balance) VALUES
(1, 1, 'Checking', 1000.00),
(2, 1, 'Savings', 5000.00),
(3, 2, 'Checking', 1500.00),
(4, 2, 'Investment', 7500.00),
(5, 3, 'Savings', 2000.00),
(6, 4, 'Checking', 3000.00),
(7, 5, 'Checking', 2500.00),
(8, 6, 'Savings', 6000.00),
(9, 7, 'Investment', 8000.00),
(10, 8, 'Checking', 1200.00);

--Insert 10 values into the Transaction table.
INSERT INTO transactions (transaction_id, account_id, transaction_date,
transaction_amount, transaction_type) VALUES
(1, 1, '2024-09-01', 200.00, 'Deposit'),
(2, 1, '2024-09-03', -100.00, 'Withdrawal'),
(3, 2, '2024-09-02', 300.00, 'Deposit'),
(4, 2, '2024-09-04', -50.00, 'Withdrawal'),
(5, 3, '2024-09-05', 150.00, 'Deposit'),
(6, 4, '2024-09-06', -200.00, 'Withdrawal'),
(7, 5, '2024-09-07', 250.00, 'Deposit'),
(8, 6, '2024-09-08', -300.00, 'Withdrawal'),
(9, 7, '2024-09-09', 400.00, 'Deposit'),
(10, 8, '2024-09-10', -150.00, 'Withdrawal');

--Insert 10 values into the loans table.
INSERT INTO loans (loan_id, customer_id, loan_amount, interest_rate,
loan_term) VALUES
(1, 1, 5000.00, 3.50, 12),
(2, 2, 7500.00, 4.00, 24),
(3, 3, 6000.00, 3.75, 18),
(4, 4, 10000.00, 4.25, 36),
(5, 5, 12000.00, 4.50, 48),
(6, 6, 8000.00, 3.90, 24),
(7, 7, 9500.00, 4.10, 30),
(8, 8, 11000.00, 4.00, 42),
(9, 9, 13000.00, 4.20, 54),
(10, 10, 7000.00, 3.85, 20);

--Insert 10 values into the loan_payments table.
INSERT INTO loan_payments (payment_id, loan_id, payment_date, payment_amount)
VALUES
(1, 1, '2024-01-15', 250.00),
(2, 1, '2024-02-15', 200.00),
(3, 2, '2024-01-20', 150.00),
(4, 2, '2024-02-20', 400.00),
(5, 3, '2024-01-25', 400.00),
(6, 3, '2024-02-25', 200.00),
(7, 4, '2024-03-01', 250.00),
(8, 4, '2024-04-01', 200.00),
(9, 5, '2024-05-10', 400.00),
(10, 5, '2024-06-10', 400.00);

SELECT * FROM customers;
SELECT * FROM accounts;
SELECT * FROM transactions;
SELECT * FROM loans;
SELECT * FROM loan_payments;

