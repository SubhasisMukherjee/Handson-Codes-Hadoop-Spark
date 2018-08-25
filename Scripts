df = spark.read.csv(path = "Online_Retail.csv", header="true", inferSchema="true", mode="DROPMALFORMED")
df.printSchema()
df.show(n=10)
df.select("InvoiceNo","StockCode","Description","InvoiceDate", 'Country')

df = df.withColumn("InvoiceNo", df['InvoiceNo'].cast('int')) \
.withColumn("Quantity", df['Quantity'].cast('int')) \
.withColumn("InvoiceDate", df['InvoiceDate'].cast('date')) \
.withColumn("UnitPrice", df['UnitPrice'].cast('float')) \
.withColumn("CustomerID", df['CustomerID'].cast('int'))

df.select("InvoiceDate").distinct().show()

df.select("InvoiceDate").distinct().count()

df.agg({'InvoiceDate':'max'}).show()

df = df.orderBy('InvoiceDate', ascending=True)
df = df.orderBy(['InvoiceDate','UnitPrice'], ascending=[True,False])

dffh = df.filter(df['InvoiceDate'] < '2011-06-31')
dffh.agg({'InvoiceDate':'max'}).show()
dffh.count()

dfsh = df.filter(df['InvoiceDate'] > '2011-06-30')
dfsh.agg({'InvoiceDate':'min'}).show()
dfsh.count()

df = df.withColumn('InvoiceSales', df['Quantity'] * df['UnitPrice'])

dfvq = df.filter(df['Quantity'] > 1)

dfvq.groupBy([df.CustomerID, df.InvoiceNo]).sum('InvoiceSales').show()

dfvq.filter((df.CustomerID == 17850) & (df.InvoiceNo == 536407)).show()

df.filter(df.InvoiceNo.isNull()).show()

df.filter(df.InvoiceNo.isNull()).select("Country").distinct().show()

dfstck = df.select(["StockCode","Description"]).distinct()
