import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import *
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# config
people_path = "datalake/people_table"
emp_path = "datalake/emp"
dep_path = "datalake/dep"
dim_path = "datalake/dimension"

emp_path_cdc = "datalake/cdc/emp"
dep_path_cdc = "datalake/cdc/dep"


# printing table
def show_table(table_path):
    print("\n Show the table:")
    df = spark.read.format("delta").load(table_path)
    df.show()


def generate_Data(table_path, table_path2, table_path3):
    
    print("\n creating table:")
    employees = [(1, "James", "", "Smith", "36636", "M", 5000, 1,222),
                 (2, "Michael", "Rose", "", "40288", "M", 4000, 2,333),
                 (3, "Robert", "", "Williams", "42114", "M", 3500, 3,444),
                 (4, "Maria", "Anne", "Jones", "39192", "F", 3800, 3,555),
                 (5, "Jen", "Mary", "Brown", "", "F", -1, 3,666)]

    schema_emp = StructType([
        StructField("id", IntegerType(), True),
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("cod_entry", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("DepID", IntegerType(), True),
        StructField("CostCenter", StringType(), True)
    ])

    df_dep = [(1, "IT"),
              (2, "Marketing"),
              (3, "Human Resources"),
              (4, "Sales")]

    schema_dep = StructType([
        StructField("id", IntegerType(), True),
        StructField("Department", StringType(), True),
    ])

    df_emp = spark.createDataFrame(data=employees, schema=schema_emp)
    df_dep = spark.createDataFrame(data=df_dep, schema=schema_dep)

    print("saving delta tables..\n")
    df_emp.write.format("delta")\
        .option("overwriteSchema", "true")\
        .mode("overwrite")\
        .save(table_path)
    
    df_dep.write.format("delta")\
        .option("overwriteSchema", "true")\
        .mode("overwrite")\
        .save(table_path2)

    print("loading delta tables..\n")
    df_emp = spark.read.format("delta").load(table_path)
    df_dep = spark.read.format("delta").load(table_path2)

    df_emp.show()
    df_dep.show()

    print("creating delta views..\n")
    df_emp.createOrReplaceTempView('employees')
    df_dep.createOrReplaceTempView('departments')

    print("dimension table..\n")
    query = spark.sql('''select e.id,
                         e.firstname, 
                         e.middlename,
                         e.salary,
                         d.Department 
                         from employees e inner join departments d 
                         on e.id = d.id''')
    query.show()

    print("saving delta table for dimension table...")
    query.write.format("delta")\
        .mode("overwrite")\
        .save(table_path3)


def updates_CDC(table_path):

    print("\n creating table:")

    data2 = [(1, "U", "James", "Ferreira", "Ferreira", "36636", "M", 10000, 2,222, "2022-09-14 13:10:12"),
             (2, "U", "Michael", "Rose", "Ferreira", "40288", "M", 4000, 1, 333, "2022-09-13 13:10:12"),
             (6, "I", "Norma", "Maria", "Santana", "40288", "F", 20000, 1, 444, "2022-09-12 13:10:12"),
             (7, "I", "Erik", "", "", "40288", "2", 50000, 1,555, "2022-09-15 13:10:12"),
             (1, "U", "James", "Ferreira", "Ferreira", "36636", "M", 10000, 2, 666, "2022-09-15 13:10:12"),
             (1, "U", "James", "Maciel", "Ferreira", "36636", "M", 10000, 2, 666,"2022-09-16 13:10:12"),
             (2, "D", "", "", "", 0, "", 0, 0, 0, "2022-09-15 13:10:12"),
             ]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("Op", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("cod_entry", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("DepID", IntegerType(), True),
        StructField("CostCenter", StringType(), True),
        StructField("TIMESTAMP", StringType(), True)
    ])

    df = spark.createDataFrame(data=data2, schema=schema)
    df.show()
    
    df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .save(table_path)


def upsert_tables(spark, table1, table2, table3, table4):
    # load table source, cdc table and dimension table..
    df_table1 = spark.read.format("delta").load(table1)
    df_table2 = spark.read.format("delta").load(table2)
    df_table3 = spark.read.format("delta").load(table3)
    df_table4 = spark.read.format("delta").load(table4)

    print("\nBefore merge...")
    
    print("\nTable source...")
    df_table1.show()
    
    print("\nTable updates...")
    df_table2.show()

    print("\nDimension table...")
    df_table3.show()

    deltaTable_1 = DeltaTable.forPath(spark, table1)
    deltaTable_2 = DeltaTable.forPath(spark, table2)
    deltaTable_2 = deltaTable_2.toDF()

    # get the last changed in table by key
    deltaTable_2 = deltaTable_2.withColumn("rn", row_number().over(Window.partitionBy('id').orderBy(col("TIMESTAMP").desc())))
    deltaTable_2 = deltaTable_2.select('*').where('rn=1')

    dfUpdates = deltaTable_2.select("id","Op","firstname","middlename","lastname","cod_entry","gender","salary","DepID","CostCenter")

    deltaTable_1.alias('table1') \
        .merge(
        dfUpdates.alias('table2'),
        'table1.id = table2.id') \
        .whenMatchedDelete(condition="table2.Op ='D'")\
        .whenMatchedUpdateAll(condition="table2.Op ='U'")\
        .whenNotMatchedInsertAll(condition="table2.Op ='I'")\
        .execute()
    
    print("\n printing the table source after merge...")
    df = deltaTable_1.toDF()
    df.show()

    # update the dimension table...

    df_table3.createOrReplaceTempView('tb_dimension')
    df_table1.createOrReplaceTempView('employees')
    df_table4.createOrReplaceTempView('departments')
    
    query = spark.sql('''select distinct e.id,
                         e.firstname,
                         e.middlename,
                         e.salary,
                         d.Department, 
                         current_timestamp() as TIMESTAMP
                         from employees e left join departments d 
                         on e.id = d.id''')
    
    print("after filter...")
    query.show()
    query.createOrReplaceTempView('tb_dimension_updates')

    spark.sql('''MERGE INTO tb_dimension
    USING tb_dimension_updates
    ON tb_dimension.id = tb_dimension_updates.id
    WHEN MATCHED THEN
    UPDATE SET
        firstName = tb_dimension_updates.firstname,
        middleName = tb_dimension_updates.middlename,
        salary = tb_dimension_updates.salary,
        Department = tb_dimension_updates.Department
    WHEN NOT MATCHED
    THEN INSERT (
        firstname,
        middleName,
        salary,
        Department
    )
    VALUES (
        tb_dimension_updates.firstName,
        tb_dimension_updates.middleName,
        tb_dimension_updates.salary,
        tb_dimension_updates.Department
    )''')

    print("\nprinting dimension table after merge:")
    query = spark.sql("select * from tb_dimension")
    query.show()
    df_table3.show()


generate_Data(emp_path, dep_path, dim_path)
updates_CDC(emp_path_cdc)
show_table(emp_path_cdc)
upsert_tables(spark, emp_path, emp_path_cdc, dim_path, dep_path)

