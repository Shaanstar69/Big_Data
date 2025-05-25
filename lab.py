import pandas as pd
import sqlite3

def process_data():

    # Loading the transactions data from the CSV file into a pandas DataFrame
    file_path = r"src/data/transactions.csv" 
    df = pd.read_csv(file_path, encoding="utf-8")
    
    # Removing any rows with missing values in the DataFrame (Use dropna or another method)
    df.dropna(inplace=True)  # You can change this to other methods if required

    # Converting the 'TransactionDate' column to a datetime format using pandas
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    # Setting up a connection to SQLite database and create a table if it doesn't exist
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        TransactionID INTEGER PRIMARY KEY,
        CustomerID INTEGER,
        Product TEXT,
        Amount REAL,
        TransactionDate TEXT,
        PaymentMethod TEXT,
        City TEXT,
        Category TEXT
    )
    """)
    
    # TO DO: Insert data into the database
    # Your task: Insert the cleaned DataFrame into the SQLite database. Ensure to replace the table if it already exists.
    df.to_sql("transactions", conn, if_exists="replace", index=False)

    # Example Queries - Write SQL queries based on the instructions below

    # TO DO: Query for Top 5 Most Sold Products
    # Your task: Write an SQL query to find the top 5 most sold products based on transaction count.
    print("\nTop 5 Most Sold Products:")
    cursor.execute("""
    SELECT product, COUNT(*) as transaction_count 
    FROM transactions 
    GROUP BY product 
    ORDER BY transaction_count DESC 
    LIMIT 5
    """)
    print(cursor.fetchall())


    # TO DO:  Query for Monthly Revenue Trend
    # Your task: Write an SQL query to find the total revenue per month.
    print("\nMonthly Revenue Trend:")
    cursor.execute("""  SELECT strftime('%Y-%m', TransactionDate) as month, 
           SUM(amount) as total_revenue 
    FROM transactions 
    GROUP BY month 
    ORDER BY month  """)
    print(cursor.fetchall())

    # TO DO:  Query for Payment Method Popularity
    # Your task: Write an SQL query to find the popularity of each payment method used in transactions.
    print("\nPayment Method Popularity:")
    cursor.execute("""  SELECT PaymentMethod, COUNT(*) as usage_count 
    FROM transactions 
    GROUP BY PaymentMethod 
    ORDER BY usage_count DESC  """)
    print(cursor.fetchall())


    # TO DO:  Query for Top 5 Cities with Most Transactions
    # Your task: Write an SQL query to find the top 5 cities with the most transactions.
    print("\nTop 5 Cities with Most Transactions:")
    cursor.execute("""  SELECT City, COUNT(*) as transaction_count 
    FROM transactions 
    GROUP BY City 
    ORDER BY transaction_count DESC 
    LIMIT 5  """)
    print(cursor.fetchall())


    # TO DO:  Query for Top 5 High-Spending Customers
    # Your task: Write an SQL query to find the top 5 customers who spent the most in total.
    print("\nTop 5 High-Spending Customers:")
    cursor.execute("""  SELECT CustomerID, SUM(amount) as total_spent 
    FROM transactions 
    GROUP BY CustomerID 
    ORDER BY total_spent DESC 
    LIMIT 5  """)
    print(cursor.fetchall())


    # TO DO:  Query for Hadoop vs Spark Related Product Sales
    # Your task: Write an SQL query to categorize products related to Hadoop and Spark and find their sales.
    print("\nHadoop vs Spark Related Product Sales:")
    cursor.execute("""  SELECT 
        CASE 
            WHEN product LIKE '%Hadoop%' THEN 'Hadoop'
            WHEN product LIKE '%Spark%' THEN 'Spark'
            ELSE 'Other'
        END as product_category,
        SUM(amount) as total_sales,
        COUNT(*) as transaction_count
    FROM transactions
    GROUP BY product_category
    HAVING product_category IN ('Hadoop', 'Spark')  """)
    print(cursor.fetchall())


    # TO DO:  Query for Top Spending Customers in Each City
    # Your task: Write an SQL query to find the top spending customer in each city using subqueries.
    print("\nTop Spending Customers in Each City:")
    cursor.execute("""  SELECT City, CustomerID, total_spent
    FROM (
        SELECT 
            City, 
            CustomerID, 
            SUM(amount) as total_spent,
            RANK() OVER (PARTITION BY City ORDER BY SUM(amount) DESC) as rank
        FROM transactions
        GROUP BY City, CustomerID
    ) 
    WHERE rank = 1
    ORDER BY total_spent DESC  """)
    print(cursor.fetchall())


    # Step 8: Close the connection
    # Your task: After all queries, make sure to commit any changes and close the connection
    conn.commit()
    conn.close()
    print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()
