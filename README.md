# WindowFunction
Find average salary of employees for each department and order employees within a department by age  
Input and putput should look as below: <br/>
<br/>
<img src="https://github.com/user-attachments/assets/26e77cb9-b1e9-474e-9c42-fd84d0fd4671" height="40%" width="40%" alt="Disk Sanitization Steps"/>  
data = [  
    ("Sudeep", 25, "Sales", 30000),  
    ("Suresh", 22, "Finance", 50000),  
    ("Pradeep", 28, "Finance", 20000),  
    ("Iqbal", 22, "Sales", 20000),  

]

columns = ["name","age","department","salary"]  
df = spark.createDataFrame(data,schema = columns)  
df.show()  

# Window spec for ordering by age within department

from pyspark.sql.window import Window  
from pyspark.sql.functions import *  

windowspec = Window.partitionBy("department").orderBy(desc("age"))

#Add both avg salary and row_number in one go  

final_df = df.withColumn("avg_salary", avg("salary").over(Window.partitionBy("department"))) \  
             .withColumn("row_num", row_number().over(windowspec))  
final_df.show()  

# Walking thru the each code

1. Create DataFrame
df = spark.createDataFrame(data, schema=columns)
df.show()



👉 Basic employee dataset with name, age, department, salary.

2. Define Window for Ordering by Age
windowspec = Window.partitionBy("department").orderBy(desc("age"))


partitionBy("department") → groups employees within the same department.

orderBy(desc("age")) → sorts employees by age descending inside each department.

Example:

Finance group → [Pradeep(28), Suresh(22), Ram(20)]

Sales group → [Sudeep(25), Iqbal(22)]

3. Add Average Salary Column
df.withColumn("avg_salary", avg("salary").over(Window.partitionBy("department")))


avg("salary") → computes average salary.

.over(Window.partitionBy("department")) → computes that average per department.

For Finance → (50000 + 50000 + 20000) / 3 = 40000
For Sales → (30000 + 20000) / 2 = 25000

So every employee gets their department’s average salary as a new column.

4. Add Row Number Column
.withColumn("row_num", row_number().over(windowspec))


row_number() → assigns a unique number starting from 1.

.over(windowspec) → applies inside each department, ordered by age desc.

So:

Finance → Pradeep(28) → 1, Suresh(22) → 2, Ram(20) → 3

Sales → Sudeep(25) → 1, Iqbal(22) → 2

5. Final Result
final_df.show()


🔹 In Plain English

Group employees by department.

For each department:

Compute average salary and add it as a column.

Assign a row number based on descending age.
