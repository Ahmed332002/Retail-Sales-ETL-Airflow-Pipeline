import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime ,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook




def transformations():

    csv_path = "/home/kiwilytics/airflow_output/retail_store_sales.csv"
    df = pd.read_csv(csv_path)

#cleaning & transformations
    df['Quantity']=pd.to_numeric(df['Quantity'], errors='coerce')
    df['Total Spent']=pd.to_numeric(df['Total Spent'], errors='coerce')
    df['Price Per Unit']=pd.to_numeric(df['Price Per Unit'], errors='coerce')
    df['Transaction Date']=pd.to_datetime(df['Transaction Date'])

#standardization

    df['Category']=df['Category'].str.upper().str.strip()
    df['Payment Method']=df['Payment Method'].str.strip().str.title()
    df['Item']=df['Item'].str.strip().str.title()
    df['Location'] = df['Location'].str.title().str.strip()



# fill nan of item
    np.random.seed(42)
    prob_item = df['Item'].value_counts(normalize=True)
    nan_count = df['Item'].isna().sum()
    fill_items = np.random.choice(prob_item.index, size=nan_count, p=prob_item.values)
    df.loc[df['Item'].isna(), 'Item'] = fill_items


    # filling nan values

    df['Quantity'] = df['Quantity'].fillna(df.groupby('Item')['Quantity'].transform('mean')).round().astype(int)
    df['Price Per Unit'] = df['Price Per Unit'].fillna(df.groupby('Item')['Price Per Unit'].transform(lambda x: x.mode()[0]))
    df['Total Spent']=df['Quantity']*df['Price Per Unit']

# fill nan of Discount Applied

    np.random.seed(42)
    prob_per_item = df.groupby('Item')['Discount Applied'].value_counts(normalize=True)
    prob_dict = {item: prob_per_item[item] for item in prob_per_item.index.levels[0]}
    nan_counts = df[df['Discount Applied'].isna()]['Item'].value_counts()
    fill_values = []
    for item, count in nan_counts.items():
        dist = prob_dict[item]
        fill_values.extend(np.random.choice(dist.index, size=count, p=dist.values))
    df.loc[df['Discount Applied'].isna(), 'Discount Applied'] = fill_values

    # cleaning

    df = df.dropna(subset=['Transaction Date'])


    df.to_csv("/home/kiwilytics/airflow_output/my_csv_table_clean.csv", index=False)

def plot_daily_revenue():
    df = pd.read_csv("/home/kiwilytics/airflow_output/my_csv_table_clean.csv")

   
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
    df["Months"] = df["Transaction Date"].dt.to_period("M")

 # monthly_revenue
    monthly_revenue = df.groupby("Months")["Total Spent"].sum()
    monthly_revenue.reset_index().to_csv(
        "/home/kiwilytics/airflow_output/monthly_revenue.csv", index=False
    )
    plt.figure(figsize=(12,6))
    monthly_revenue.plot(kind='line', marker='o')
    plt.title("Monthly Revenue")
    plt.xlabel("Month")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.savefig("/home/kiwilytics/airflow_output/monthly_revenue.png")
    plt.close()

 # monthly_orders
    monthly_orders = df.groupby("Months").size()
    plt.figure(figsize=(12,6))
    monthly_orders.plot(kind='bar' , color=["#e376d2"])
    plt.title("Monthly Orders Count")
    plt.xlabel("Month")
    plt.ylabel("Number of Orders")
    plt.xticks(rotation=45)
    plt.savefig("/home/kiwilytics/airflow_output/monthly_orders.png")
    plt.close()

 # revenue_by_category 
    revenue_by_category = df.groupby("Category")["Total Spent"].sum().sort_values(ascending=False)
    plt.figure(figsize=(14,8))
    revenue_by_category.plot(kind='bar')
    plt.title("Revenue by Category")
    plt.xlabel("Category")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=60, ha='right')   
    plt.tight_layout()                    
    plt.savefig("/home/kiwilytics/airflow_output/revenue_by_category.png")
    plt.close()


 # payment_counts
    payment_counts = df["Payment Method"].value_counts()
    plt.figure(figsize=(6,6))
    payment_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140 , colors=["#37033E", "#06eed3", "#dd107d"])
    plt.title("Payment Method Distribution")
    plt.ylabel("")
    plt.savefig("/home/kiwilytics/airflow_output/payment_distribution.png")
    plt.close()

 # top_items
    top_items = df["Item"].value_counts().head(10)
    plt.figure(figsize=(12,6))
    top_items.plot(kind='barh', color=["#24dce3"])
    plt.title("Top 10 Selling Items")
    plt.xlabel("Item")
    plt.ylabel("Number of Sales")
    plt.xticks(rotation=45)
    plt.savefig("/home/kiwilytics/airflow_output/top_items.png")
    plt.close()

 #daily_revenue 

    daily_revenue = df.groupby(df["Transaction Date"].dt.date)["Total Spent"].sum()
    plt.figure(figsize=(12,6))
    daily_revenue.plot(kind='line')
    plt.title("Daily Revenue Trend")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.savefig("/home/kiwilytics/airflow_output/daily_revenue.png")
    plt.close()

    


with DAG(
dag_id='first_dag',
description='this is first time to use this dag',
start_date=datetime(2025,1,3),
schedule_interval=timedelta(minutes=15),
catchup=False,
dagrun_timeout=timedelta(minutes=30),
) as dag:
   

  
    transformation_task=PythonOperator(
    task_id='transformation_task',
    python_callable=transformations
       )

    plot_daily_revenue_task = PythonOperator(
    task_id="plot_daily_revenue_task",
    python_callable=plot_daily_revenue
)
 

transformation_task >> plot_daily_revenue_task 

