

#import grequests
#import pandas as pd
# import requests
# from io import BytesIO
# import gzip

from flask import Flask, render_template, session, jsonify, request
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf, concat, lit, split, when, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql import Window

from data_processing import fetch_and_process_data

app = Flask(__name__)
app.secret_key = 'vitDSession'

# # Initialize Spark session
# spark = SparkSession.builder.appName("PrescribingAnalysis").config("spark.driver.memory", "4g").config("spark.executor.memory", "8g").getOrCreate()
spark = SparkSession.builder.appName("PrescribingAnalysis").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", -1)

# TO SEPERATE MEDICINE ON THE BASIS OF ITS FORM 
# Define a UDF to categorize medication types based on keywords
def categorize_medication_type(description):
    keywords = {
        "liquid": ["liquid", "drink", "solution", "syrup", "bottle","drops","drop","soln","oral suspension","dps"],
        "powder": ["powder", "sachets", "sach"],
        "capsules": ["capsules", "capsule"],
        "tablets": ["tablets", "tablet", "caplets", "tab"],
        "chewable tablets": ["chewable", "chew tab"],
        "injection": ["inj"],
        "spray": ["spray"]
    }
    description_lower = description.lower()
    for med_type, med_keywords in keywords.items():
        if any(keyword in description_lower for keyword in med_keywords):
            return med_type
    return None

# Define a UDF (User-Defined Function) to extract the medication and dosage
def extract_medication_and_dosage(description):
    words = description.split(' ')
    medication = ''
    dosage = ''
    
    for word in words:
        if word[0].isdigit():
            break
        else:
            medication += word + ' '

    if medication.strip():
        dosage = ' '.join(words[len(medication.split()):])
    
    return (medication.strip(), dosage.strip())

# Create a UDF from the defined function
extract_udf = F.udf(extract_medication_and_dosage, returnType=StructType([
    StructField("medication", StringType(), True),
    StructField("dosage", StringType(), True)
]))

# Call fetch_and_process_data only if 'async_df' is not in the session or is empty
with app.test_request_context('/'):  # Create a temporary request context
    if 'prescriptions_df' not in session or session['prescriptions_df'].isEmpty():
        prescriptions_df = fetch_and_process_data()
        # Store 'async_df' in the session
        session['prescriptions_df'] = prescriptions_df
    else:
        prescriptions_df = session['prescriptions_df']
# prescriptions_df = fetch_and_process_data()

total_items = prescriptions_df.count()

# Create a UDF for Spark FOR MEDICATION TYPE
udf_categorize_medication_type = udf(categorize_medication_type, StringType())
# Apply the UDF to create a new column 'medication_type'
prescriptions_df = prescriptions_df.withColumn("medication_type", udf_categorize_medication_type(col("BNF_DESCRIPTION")))
# Extract year from YEAR_MONTH and create a new column "year"
prescriptions_df = prescriptions_df.withColumn("year", F.year(F.from_unixtime(F.unix_timestamp(F.col("YEAR_MONTH").cast("string"), "yyyyMM"))))
prescriptions_df = prescriptions_df.withColumn("month", F.month(F.from_unixtime(F.unix_timestamp(F.col("YEAR_MONTH").cast("string"), "yyyyMM"))))
# Apply the UDF to create new columns
prescriptions_df = prescriptions_df.withColumn("extracted_data", extract_udf("BNF_DESCRIPTION"))
prescriptions_df = prescriptions_df.withColumn("medication", col("extracted_data.medication"))
prescriptions_df = prescriptions_df.withColumn("dosage", col("extracted_data.dosage"))
liquid_rows = prescriptions_df.filter(col("medication_type") == "liquid")

def get_all_data(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "year","month", "PRACTICE_NAME", "PRACTICE_CODE","POSTCODE","BNF_DESCRIPTION", 
        "BNF_CHAPTER_PLUS_CODE", "medication_type","medication", "dosage",
        "QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.select(*columns_to_select)

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query.filter(
            (col("year") == selected_year)
        )

    if selected_month != "0":
        base_query = base_query.filter(
            (col("month") == selected_month) 
        )
    
    # Define a window specification with partition_column
    window_spec_all_data = Window.partitionBy("year", "month").orderBy("year", "month", "PRACTICE_NAME", "BNF_DESCRIPTION")

    # Add a row number to each row based on the window specification
    base_query = base_query.withColumn("row_number", F.row_number().over(window_spec_all_data))

    # Apply distinct if needed
    base_query = base_query.distinct()

    # Calculate the total number of records
    total_records = base_query.count()

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size

    # Apply pagination
    result = base_query.filter(col("row_number").between(offset + 1, offset + page_size)).limit(page_size) 

    # Extracting columns and filtered data
    columns = columns_to_select
    filtered_data = [row.asDict() for row in result.collect()]

    return columns, filtered_data , total_pages, page_number 

def get_bnf_descriptions(selected_year, selected_month, page_number, page_size=10):
    
    # Define the base selection of columns
    columns_to_select = [
        "BNF_DESCRIPTION","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = liquid_rows.select(*columns_to_select)

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query.filter(
            (col("year") == selected_year)
        )

    if selected_month != "0":
        base_query = base_query.filter(
            (col("month") == selected_month) 
        )

    # Apply distinct if needed
    base_query = base_query.groupBy("BNF_DESCRIPTION").agg(
        F.sum("QUANTITY").alias("sum_quantity"),
        F.sum("ITEMS").alias("sum_items"),
        F.sum("TOTAL_QUANTITY").alias("sum_total_quantity"),
        F.sum("NIC").alias("sum_NIC"),
        F.sum("ACTUAL_COST").alias("sum_actual_cost"),
        F.count("*").alias("count")
    )
    
    # Define a window specification with partition_column
    window_spec_all_data = Window.orderBy("BNF_DESCRIPTION", "sum_items", "count")

    # Add a row number to each row based on the window specification
    base_query = base_query.withColumn("row_number", F.row_number().over(window_spec_all_data))

    # Apply distinct if needed
    base_query = base_query.distinct()

    # Calculate the total number of records
    total_records = base_query.count()

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.filter(col("row_number").between(offset + 1, offset + page_size)).limit(page_size) 

    # Extracting columns and filtered data
    columns = ["BNF_DESCRIPTION", "sum_quantity", "sum_items", "sum_total_quantity","sum_NIC", "sum_actual_cost", "count"]
    filtered_data = [row.asDict() for row in result.collect()]

    return columns, filtered_data , total_pages, page_number 
 
def get_BNF_CHAPTER_PLUS_CODE(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "BNF_CHAPTER_PLUS_CODE","medication_type","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.select(*columns_to_select)

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query.filter(
            (col("year") == selected_year)
        )

    if selected_month != "0":
        base_query = base_query.filter(
            (col("month") == selected_month) 
        )

    # Apply distinct if needed
    base_query = base_query.groupBy("BNF_CHAPTER_PLUS_CODE","medication_type").agg(
        F.sum("QUANTITY").alias("sum_quantity"),
        F.sum("ITEMS").alias("sum_items"),
        F.sum("TOTAL_QUANTITY").alias("sum_total_quantity"),
        F.sum("NIC").alias("sum_NIC"),
        F.sum("ACTUAL_COST").alias("sum_actual_cost"),
        F.count("*").alias("count")
    )
    
    # Define a window specification with partition_column
    window_spec_all_data = Window.orderBy("BNF_CHAPTER_PLUS_CODE","medication_type", "sum_items", "count")

    # Add a row number to each row based on the window specification
    base_query = base_query.withColumn("row_number", F.row_number().over(window_spec_all_data))

    # Apply distinct if needed
    base_query = base_query.distinct()

    # Calculate the total number of records
    total_records = base_query.count()

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.filter(col("row_number").between(offset + 1, offset + page_size)).limit(page_size) 
    
    # Extracting columns and filtered data
    columns = ["BNF_CHAPTER_PLUS_CODE","medication_type", "sum_quantity", "sum_items", "sum_total_quantity","sum_NIC", "sum_actual_cost", "count"]
    filtered_data = [row.asDict() for row in result.collect()]

    return columns, filtered_data , total_pages, page_number 
 
def get_MEDICATION_Name(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "medication","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = liquid_rows.select(*columns_to_select)

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query.filter(
            (col("year") == selected_year)
        )

    if selected_month != "0":
        base_query = base_query.filter(
            (col("month") == selected_month) 
        )

    # Apply distinct if needed
    base_query = base_query.groupBy("medication").agg(
        F.sum("QUANTITY").alias("sum_quantity"),
        F.sum("ITEMS").alias("sum_items"),
        F.sum("TOTAL_QUANTITY").alias("sum_total_quantity"),
        F.sum("NIC").alias("sum_NIC"),
        F.sum("ACTUAL_COST").alias("sum_actual_cost"),
        F.count("*").alias("count")
    )
    
    # Define a window specification with partition_column
    window_spec_all_data = Window.orderBy("medication", "sum_items", "count")

    # Add a row number to each row based on the window specification
    base_query = base_query.withColumn("row_number", F.row_number().over(window_spec_all_data))

    # Apply distinct if needed
    base_query = base_query.distinct()

    # Calculate the total number of records
    total_records = base_query.count()

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.filter(col("row_number").between(offset + 1, offset + page_size)).limit(page_size) 
    
    # Extracting columns and filtered data
    columns = ["medication", "sum_quantity", "sum_items", "sum_total_quantity","sum_NIC", "sum_actual_cost", "count"]
    filtered_data = [row.asDict() for row in result.collect()]

    return columns, filtered_data , total_pages, page_number 
 
def get_MEDICATION_Type(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "medication","medication_type","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.select(*columns_to_select)

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query.filter(
            (col("year") == selected_year)
        )

    if selected_month != "0":
        base_query = base_query.filter(
            (col("month") == selected_month) 
        )

    # Apply distinct if needed
    base_query = base_query.groupBy("medication","medication_type").agg(
        F.sum("QUANTITY").alias("sum_quantity"),
        F.sum("ITEMS").alias("sum_items"),
        F.sum("TOTAL_QUANTITY").alias("sum_total_quantity"),
        F.sum("NIC").alias("sum_NIC"),
        F.sum("ACTUAL_COST").alias("sum_actual_cost"),
        F.count("*").alias("count")
    )
    
    # Define a window specification with partition_column
    window_spec_all_data = Window.orderBy("medication","medication_type", "sum_items", "count")

    # Add a row number to each row based on the window specification
    base_query = base_query.withColumn("row_number", F.row_number().over(window_spec_all_data))

    # Apply distinct if needed
    base_query = base_query.distinct()

    # Calculate the total number of records
    total_records = base_query.count()

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.filter(col("row_number").between(offset + 1, offset + page_size)).limit(page_size) 
    
    # Extracting columns and filtered data
    columns = ["medication","medication_type", "sum_quantity", "sum_items", "sum_total_quantity","sum_NIC", "sum_actual_cost", "count"]
    filtered_data = [row.asDict() for row in result.collect()]

    return columns, filtered_data , total_pages, page_number

# Define routes and views
@app.route('/', methods=['GET'])
def home():
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        selected_value = request.args.get('selectedValue')
        selected_year = request.args.get('selectedYear')
        selected_month = request.args.get('selectedMonth')
        page_number = int(request.args.get('page_number', 1))
        page_size = int(request.args.get('page_size', 10))
        total_pages = 1
        columns = [] 
        filtered_data = [] 
        current_page = 1

        # Fetch dynamic columns and data based on the selected value
        if selected_value == "1" :
            columns, filtered_data, total_pages, current_page = get_all_data(selected_year,selected_month, page_number, page_size)
        elif selected_value == "2" :
            columns, filtered_data, total_pages, current_page = get_bnf_descriptions(selected_year,selected_month, page_number, page_size)
        elif selected_value == "3" :
            columns, filtered_data, total_pages, current_page = get_BNF_CHAPTER_PLUS_CODE(selected_year,selected_month, page_number, page_size)
        elif selected_value == "4" :
            columns, filtered_data, total_pages, current_page = get_MEDICATION_Name(selected_year,selected_month, page_number, page_size)
        elif selected_value == "5" :
            columns, filtered_data, total_pages, current_page = get_MEDICATION_Type(selected_year,selected_month, page_number, page_size)
    

        # Calculate prev_page_number and next_page_number
        prev_page_number = max(1, page_number - 1)
        next_page_number = min(total_pages, page_number + 1)

        # Prepare response
        response = { 'columns': columns, 'rows': filtered_data}
        
        
        all_data_param = {
            'prev_page_number': prev_page_number,
            'next_page_number': next_page_number,
            'page_number': current_page,
            'pages': total_pages
        }
        return jsonify({'response': response, 'all_data_param': all_data_param})
    else:
        return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, threaded=False)
