from pyspark.sql import Row

# Read from HDFS
students = sc.textFile('student_data.txt')

# Split lines into list of fields
parts = students.map(lambda l: l.split(","))

# Create Row out of each line
students_rows = parts.map(lambda p: Row(sno=p[0], fn=p[1], ln=p[2], age=p[3], mob=p[4], state=p[5], marks=p[6]))

# Create dataframe
students_df = spark.createDataFrame(students_rows)

# Create temp view
students_df.createOrReplaceTempView("students")

# Run queries
tot_marks_df = spark.sql("SELECT fn,ln,sum(marks) as tot_marks FROM students GROUP BY fn,ln")

# Show result
tot_marks_df.show()

+--------+-----------+---------+
|      fn|         ln|tot_marks|
+--------+-----------+---------+
|  Rounak|   Mohanthy|     77.0|
| Trupthi|   Mohanthy|    147.0|
|  sandip|Battacharya|    192.0|
|siddarth|Battacharya|     66.0|
|Bharathi|  Nambiayar|     54.0|
|  Rajesh|     Khanna|    169.0|
| Archana|     Mishra|     91.0|
| Preethi|    Agarwal|    237.0|
| Revanti|      Sinha|    255.0|
| supriyo|        Roy|     80.0|
|   Komal|      Nayak|     89.0|
|   Rajiv|      Reddy|    100.0|
+--------+-----------+---------+


# Write query output to HDFS
tot_marks_df.write.csv('tot_marks.csv')


tot_marks_df.write. \
format("com.databricks.spark.csv"). \
option("header", "true"). \
save("myfile.csv")




