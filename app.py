from flask import Flask, render_template, session, jsonify, request

import pandas as pd
from data_processing import fetch_and_process_data

app = Flask(__name__)
app.secret_key = 'vitDSession'

# Fetch and process data
# prescriptions_df = fetch_and_process_data() 

csv_file_path = "data/nhs.csv"
prescriptions_df = pd.read_csv(csv_file_path)

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
    
    # return (medication.strip(), dosage.strip())
    return pd.Series([medication.strip(), dosage.strip()])

# # Create a UDF from the defined function
# extract_udf = F.udf(extract_medication_and_dosage, returnType=StructType([
#     StructField("medication", StringType(), True),
#     StructField("dosage", StringType(), True)
# ]))

# # Call fetch_and_process_data only if 'async_df' is not in the session or is empty
# with app.test_request_context('/'):  # Create a temporary request context
#     if 'prescriptions_df' not in session or session['prescriptions_df'].isEmpty():
#         prescriptions_df = fetch_and_process_data()
#         # Store 'async_df' in the session
#         session['prescriptions_df'] = prescriptions_df
#     else:
#         prescriptions_df = session['prescriptions_df']

# total_items = prescriptions_df.count()

# # Create a UDF for Spark FOR MEDICATION TYPE
# udf_categorize_medication_type = udf(categorize_medication_type, StringType())
# Apply the UDF to create a new column 'medication_type'
# prescriptions_df = prescriptions_df.withColumn("medication_type", udf_categorize_medication_type(col("BNF_DESCRIPTION")))
prescriptions_df['medication_type'] = prescriptions_df['BNF_DESCRIPTION'].apply(categorize_medication_type)
# Extract year from YEAR_MONTH and create a new column "year"
# prescriptions_df = prescriptions_df.withColumn("year", F.year(F.from_unixtime(F.unix_timestamp(F.col("YEAR_MONTH").cast("string"), "yyyyMM"))))
# prescriptions_df = prescriptions_df.withColumn("month", F.month(F.from_unixtime(F.unix_timestamp(F.col("YEAR_MONTH").cast("string"), "yyyyMM"))))
prescriptions_df['YEAR_MONTH'] = pd.to_datetime(prescriptions_df['YEAR_MONTH'], format='%Y%m')
prescriptions_df['year'] = prescriptions_df['YEAR_MONTH'].dt.year
prescriptions_df['month'] = prescriptions_df['YEAR_MONTH'].dt.month
# Apply the UDF to create new columns
# prescriptions_df = prescriptions_df.withColumn("extracted_data", extract_udf("BNF_DESCRIPTION"))
# prescriptions_df = prescriptions_df.withColumn("medication", col("extracted_data.medication"))
# prescriptions_df = prescriptions_df.withColumn("dosage", col("extracted_data.dosage"))
prescriptions_df[['medication', 'dosage']] = prescriptions_df['BNF_DESCRIPTION'].apply(extract_medication_and_dosage)

# liquid_rows = prescriptions_df.filter(col("medication_type") == "liquid")
liquid_rows = prescriptions_df[prescriptions_df['medication_type'] == "liquid"]

def get_all_data(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "year","month", "PRACTICE_NAME", "PRACTICE_CODE","POSTCODE","BNF_DESCRIPTION", 
        "BNF_CHAPTER_PLUS_CODE", "medication_type","medication", "dosage",
        "QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    # base_query = prescriptions_df.select(*columns_to_select)
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    # if selected_year != "0":
    #     base_query = base_query.filter(
    #         (col("year") == selected_year)
    #     )
    if selected_year != "0":
        base_query = base_query[base_query['year'] == int(selected_year)]


    # if selected_month != "0":
    #     base_query = base_query.filter(
    #         (col("month") == selected_month) 
    #     )
    if selected_month != "0":
        base_query = base_query[base_query['month'] == int(selected_month)]
    
    # Define a window specification with partition_column
    # window_spec_all_data = Window.partitionBy("year", "month").orderBy("year", "month", "PRACTICE_NAME", "BNF_DESCRIPTION")
    window_spec_all_data = ['year', 'month', 'PRACTICE_NAME', 'BNF_DESCRIPTION']


    # Add a row number to each row based on the window specification
    # base_query = base_query.withColumn("row_number", F.row_number().over(window_spec_all_data))
    base_query['row_number'] = base_query.groupby(window_spec_all_data).cumcount() + 1

    # Apply distinct if needed
    # base_query = base_query.distinct()
    base_query = base_query.drop_duplicates(subset=columns_to_select)

    # Calculate the total number of records
    # total_records = base_query.count()
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size

    # Apply pagination
    # result = base_query.filter(col("row_number").between(offset + 1, offset + page_size)).limit(page_size) 
    result = base_query.iloc[offset:offset + page_size]

    # Extracting columns and filtered data
    columns = columns_to_select
    # filtered_data = [row.asDict() for row in result.collect()]
    filtered_data = result.to_dict(orient='records')

    return columns, filtered_data , total_pages, page_number 

def get_bnf_descriptions(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "BNF_DESCRIPTION","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = liquid_rows.copy()

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query[base_query['year'] == int(selected_year)]
    if selected_month != "0":
        base_query = base_query[base_query['month'] == int(selected_month)]
        
    # Filter out rows with null or undefined values in the 'BNF_DESCRIPTION' column
    base_query = base_query.groupby('BNF_DESCRIPTION').agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    # Rename the columns
    base_query.columns = ['BNF_DESCRIPTION'] + [f'{col}' for col in base_query.columns[1:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.iloc[offset: offset + page_size]

    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    filtered_data = result.to_dict(orient='records')

    return columns, filtered_data , total_pages, page_number 
 
def get_BNF_CHAPTER_PLUS_CODE(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "BNF_CHAPTER_PLUS_CODE","medication_type","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query[base_query['year'] == int(selected_year)]
    if selected_month != "0":
        base_query = base_query[base_query['month'] == int(selected_month)]
    
    # Apply distinct if needed
    base_query = base_query.groupby('BNF_CHAPTER_PLUS_CODE').agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    # Rename the columns
    base_query.columns = ['BNF_CHAPTER_PLUS_CODE'] + [f'{col}' for col in base_query.columns[1:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.iloc[offset: offset + page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    filtered_data = result.to_dict(orient='records')

    return columns, filtered_data , total_pages, page_number 
 
def get_MEDICATION_Name(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "medication","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = liquid_rows.copy()
    
    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query[base_query['year'] == int(selected_year)]
    if selected_month != "0":
        base_query = base_query[base_query['month'] == int(selected_month)]
    
    # # Apply distinct if needed
    base_query = base_query.groupby('medication').agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    # Rename the columns
    base_query.columns = ['medication'] + [f'{col}' for col in base_query.columns[1:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.iloc[offset: offset + page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    filtered_data = result.to_dict(orient='records')

    return columns, filtered_data , total_pages, page_number 
 
def get_MEDICATION_Type(selected_year, selected_month, page_number, page_size=10):
    # Define the base selection of columns
    columns_to_select = [
        "medication","medication_type","QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if selected_year != "0":
        base_query = base_query[base_query['year'] == int(selected_year)]
    if selected_month != "0":
        base_query = base_query[base_query['month'] == int(selected_month)]

    # Apply distinct if needed
    base_query = base_query.groupby(['medication','medication_type']).agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    # Rename the columns
    base_query.columns = ['medication','medication_type'] + [f'{col}' for col in base_query.columns[2:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + page_size - 1) // page_size

    # Calculate the offset based on page_number and page_size
    offset = (page_number - 1) * page_size
    
    # Apply pagination
    result = base_query.iloc[offset: offset + page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    filtered_data = result.to_dict(orient='records')

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
