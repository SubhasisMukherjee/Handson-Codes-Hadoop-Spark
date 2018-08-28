# Load data from local file system
stocks = spark.read \
        .format("csv") \
        .options(inferSchema = True) \
        .options(header = True) \
        .load("datasets/stocks.csv") \
        .cache()
stocks.show()

# Prit Schema
stocks.printSchema()

# Change date column's datatype to date 
stocks = stocks.withColumn("date", stocks.date.cast("date"))
stocks.show()

# Get largest value in date column
stocks.orderBy(stocks.date.desc()).show(1)

# Register the stocks dataframe as stocks temporary view.
stocks.createOrReplaceTempView("stocks")

# Create a new dataframe, stocks_last10 with last 10 records for each stock. Select date, symbol and adjclose columns
stocks_last10 = sql("""
select date, symbol, adjclose, rn
from (select date, symbol, adjclose , row_number() over (partition by symbol order by date  desc) rn
      from stocks) tmp
where tmp.rn <= 10
""")
stocks_last10.show()

# Create a new dataframe stocks_pivot, by pivoting the stocks_last10 dataframe
stocks_pivot = stocks_last10.groupby("symbol").pivot("date").agg({"adjclose": "max"})
stocks_pivot.show()

# Prepare new column names and remane
columns = ["v" + str(i) for i in range(len(stocks_pivot.columns) - 1)]
columns.insert(0, "symbol")
columns
stocks_pivot = stocks_pivot.toDF(*columns)
stocks_pivot.show()


