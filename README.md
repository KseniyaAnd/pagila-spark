# PySpark Pagila Analytics

A data analytics project built with **Python, Apache Spark (PySpark), and PostgreSQL**.

The project uses the Pagila sample database to practice data processing and analytical queries with PySpark. It includes seven tasks involving table joins, aggregations, filtering, window functions, and ranking.

## Technologies

- Python
- Apache Spark
- PySpark
- PostgreSQL
- JDBC
- python-dotenv

## Project Structure

```text
pyspark-pagila/
│
├── tasks/
│   ├── __init__.py
│   ├── task_01.py
│   ├── task_02.py
│   ├── task_03.py
│   ├── task_04.py
│   ├── task_05.py
│   ├── task_06.py
│   └── task_07.py
│
├── .gitignore
├── requirements.txt
├── database.py
├── spark.py
└── main.py
```

## Database

The project uses the **Pagila** sample database, which represents a DVD rental store.

Data is retrieved from PostgreSQL using Spark JDBC.

The following tables are used:

- `actor`
- `address`
- `category`
- `city`
- `customer`
- `film`
- `film_actor`
- `film_category`
- `inventory`
- `payment`
- `rental`

## Tasks

### Task 01 – Movies per Category

Count the number of movies in each category and sort the results in descending order.

**Concepts:** JOIN, GROUP BY, COUNT, ORDER BY.

### Task 02 – Top 10 Most Rented Actors

Find the 10 actors whose movies were rented most frequently.

**Concepts:** Multiple JOINs, GROUP BY, COUNT, ORDER BY, LIMIT.

### Task 03 – Highest Revenue Category

Find the movie category that generated the highest total rental payment amount.

**Concepts:** Multiple JOINs, GROUP BY, SUM, ORDER BY, LIMIT.

### Task 04 – Movies Not in Inventory

Find movie titles that do not have corresponding records in the inventory table.

**Concepts:** LEFT ANTI JOIN, SELECT, ORDER BY.

### Task 05 – Top 3 Actors in the Children Category

Find the top 3 actors who appeared in the most movies in the Children category.

If multiple actors have the same number of movies, all actors with the corresponding rank are included.

**Concepts:** JOIN, FILTER, COUNT DISTINCT, Window Functions, DENSE_RANK.

### Task 06 – Active and Inactive Customers by City

Count active and inactive customers in each city.

A customer is considered active when `customer.active = 1` and inactive when `customer.active = 0`.

Sort the results by the number of inactive customers in descending order.

**Concepts:** JOIN, GROUP BY, SUM, WHEN, ORDER BY.

### Task 07 – Rental Hours by City Group and Category

Find the movie category with the highest total rental duration in two groups of cities:

- Cities starting with the letter "A".
- Cities containing the "-" symbol.

Only completed rentals are considered. Rental duration is calculated in hours using `return_date` and `rental_date`.

If multiple categories have the same maximum rental duration, all of them are included.

**Concepts:** Multiple JOINs, FILTER, WHEN, Date and Time Operations, GROUP BY, Window Functions, DENSE_RANK.

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/pyspark-pagila.git
cd pyspark-pagila
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

Activate it:

**Windows:**

```powershell
.venv\Scripts\activate
```

**Linux / macOS:**

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root directory using `.env.example` as a template.

Set your PostgreSQL connection details:

```env
JDBC_URL=jdbc:postgresql://localhost:5433/pagila
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=org.postgresql.Driver
```

Make sure PostgreSQL is running and the Pagila database is available.

### 5. Configure PostgreSQL JDBC Driver

Apache Spark requires the PostgreSQL JDBC Driver to connect to the database.

Make sure the PostgreSQL JDBC driver JAR is available to Spark when creating the SparkSession.

### 6. Run the project

```bash
python main.py
```

All seven tasks will be executed sequentially, and the results will be displayed in the terminal.

## Learning Objectives

This project was created to practice and improve knowledge of:

- Working with SparkSession and DataFrames.
- Reading data from PostgreSQL using JDBC.
- Joining multiple tables.
- Filtering and transforming data.
- Grouping and aggregating data.
- Using built-in PySpark SQL functions.
- Working with Window Functions.
- Applying ranking functions such as `DENSE_RANK`.
- Handling date and time calculations.
- Organizing PySpark code into reusable modules.
