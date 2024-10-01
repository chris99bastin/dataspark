import pandas as pd
import mysql.connector
import re

#SQLConnection
connection = mysql.connector.connect(
    host="localhost",
    user="root",                
    password="chris",
    database="dataspark"
)
datas=connection.cursor()

# read Customer Data
customer_path=open('C:\\Users\\Bastin\\Desktop\\Christy\\dataspark\\Customers.csv')
df1=pd.read_csv(customer_path)
df1['Birthday'] = pd.to_datetime(df1['Birthday'], format='%m/%d/%Y')  # Adjust format according to your date format

# Ensure it's in the correct string format before inserting into MySQL
df1['Birthday'] = df1['Birthday'].dt.strftime('%Y-%m-%d')
df1 = df1.where(pd.notnull(df1), None)
df1= df1.dropna()
df1['Birthday'] = df1['Birthday'].fillna('1900-01-01')  # Example for date field
df1['Gender'] = df1['Gender'].fillna('Unknown')
df1['State Code'] = df1['State Code'].str[:2]
long_state_codes = df1[df1['State Code'].str.len() > 2]
df1['State Code'] = df1['State Code'].fillna('XX') 
# print(df1)

# Read Product Data
product_path=open('C:\\Users\\Bastin\\Desktop\\Christy\\dataspark\\Products.csv')
df2=pd.read_csv(product_path)
df2['Product Name']= df2.get('Product Name', 'Default_Value')

if df2['Product Name'].iloc[0] in df2['Product Name']:  # Access a single value
  pass

df2['Unit Cost USD']=df2['Unit Cost USD'].str.replace(r'[$]', '', regex=True).str.replace(',', '').str.strip() 
df2['Unit Cost USD']=df2['Unit Cost USD'].astype(float)    
df2['Unit Price USD']=df2['Unit Price USD'].str.replace(r'[$]', '', regex=True).str.replace(',', '').str.strip() 
df2['Unit Price USD']=df2['Unit Price USD'].astype(float)
print(df2)

# read Store Data
stores_path=open('C:\\Users\\Bastin\\Desktop\\Christy\\dataspark\\Stores.csv')
df4=pd.read_csv(stores_path)
df4['Open Date'] = pd.to_datetime(df4['Open Date'], format='%m/%d/%Y')
df4['Open Date'] = df4['Open Date'].dt.strftime('%Y-%m-%d')
median_value = df4['Square Meters'].median()
df4['Square Meters'] = df4['Square Meters'].fillna(median_value)         # 0 for missing meters
# print(df4)

#Read Sales Data
sales_path=open('C:\\Users\\Bastin\\Desktop\\Christy\\dataspark\\Sales.csv')
df3=pd.read_csv(sales_path)
df3.columns = df3.columns.str.strip().str.replace(r'\t', '', regex=True)
df3['Delivery Date'].replace('NaN', None, inplace=True)
df3['Delivery Date'] = pd.to_datetime(df3['Delivery Date'], errors='coerce')
df3['Delivery Date'] = df3['Delivery Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
df3['Order Date'] = pd.to_datetime(df3['Order Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
df3['OrderDate'] = pd.to_datetime(df3['Order Date'], errors='coerce')
# print(df3)

for index, row in df1.iterrows():
    sql = "INSERT INTO Customers (CustomerKey,Gender, Name, City, StateCode,State,ZipCode,Country,Continent,Birthday) VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"
    vals = (row['CustomerKey'], row['Gender'], row['Name'], row['City'],row['State Code'], row['State'], row['Zip Code'],
            row['Country'],row['Continent'],row['Birthday'])
    datas.execute(sql, vals)
    


# Insert data into the 'Products' table
for index, row in df2.iterrows():
    sql = "INSERT INTO Products (ProductKey, ProductName,Brand,Color, UnitCostUSD, UnitPriceUSD,SubcategoryKey,Subcategory,CategoryKey, Category) VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"
    vals = (row['ProductKey'],row['Product Name'],row['Brand'],row['Color'], row['Unit Cost USD'], row['Unit Price USD'],row['SubcategoryKey'],row['Subcategory'],row['CategoryKey'],row['Category'])
    datas.execute(sql, vals)
    
# Insert data into the 'Stores' table
for index, row in df4.iterrows():
    sql = "INSERT INTO Stores (StoreKey, Country, State, SquareMeters,OpenDate) VALUES (%s, %s, %s, %s, %s)"
    vals = (row['StoreKey'], row['Country'], row['State'], row['Square Meters'],row['Open Date'])
    datas.execute(sql, vals)

# Insert data into the 'Sales' table
for index, row in df3.iterrows():
    # Check if CustomerKey exists in customers table
    check_sql = "SELECT COUNT(*) FROM customers WHERE CustomerKey = %s"
    datas.execute(check_sql, (row['CustomerKey'],))
    customer_exists = datas.fetchone()[0]
    
    if customer_exists:  # Only insert if CustomerKey exists in customers
        sql = """
        INSERT INTO Sales (OrderNumber, LineItem, OrderDate, DeliveryDate, CustomerKey, StoreKey, ProductKey, Quantity, CurrencyCode) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            OrderDate = VALUES(OrderDate),
            DeliveryDate = VALUES(DeliveryDate),
            StoreKey = VALUES(StoreKey),
            ProductKey = VALUES(ProductKey),
            Quantity = VALUES(Quantity),
            CurrencyCode = VALUES(CurrencyCode);
        """
        vals = (
            row['Order Number'], 
            row['Line Item'], 
            row['Order Date'], 
            row['Delivery Date'], 
            row['CustomerKey'], 
            row['StoreKey'], 
            row['ProductKey'], 
            row['Quantity'], 
            row['Currency Code']
        )
        datas.execute(sql, vals)
    else:
        print(f"CustomerKey {row['CustomerKey']} does not exist. Skipping this row.")

# Commit the transaction
connection.commit()

# Close the connection
datas.close()
connection.close()
