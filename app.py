from flask import Flask, render_template, session, jsonify, request

import pandas as pd
import glob
import os
from data_processing import fetch_and_process_data
from classes.nhs_param import NHSParam
from datetime import datetime
# from reportlab.lib import colors
# from reportlab.lib.pagesizes import letter,landscape
# from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
# from reportlab.pdfgen import canvas

app = Flask(__name__)
app.secret_key = 'vitDSession'

# Fetch and process data
# prescriptions_df = fetch_and_process_data() 

data_folder = "data"
file_prefix = "output_"
csv_files = glob.glob(os.path.join(data_folder, f"{file_prefix}*.csv"))
if csv_files:
    # Extract modification times for each file
    file_modification_times = [(f, os.path.getmtime(f)) for f in csv_files]

    # Sort files by modification time and get the latest one
    latest_csv_file, _ = max(file_modification_times, key=lambda x: x[1])

    # Read the CSV file into a Pandas DataFrame
    prescriptions_df = pd.read_csv(latest_csv_file)
else:
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
        
    return pd.Series([medication.strip(), dosage.strip()])

prescriptions_df['medication_type'] = prescriptions_df['BNF_DESCRIPTION'].apply(categorize_medication_type)
prescriptions_df['YEAR_MONTH'] = pd.to_datetime(prescriptions_df['YEAR_MONTH'], format='%Y%m')
prescriptions_df['year'] = prescriptions_df['YEAR_MONTH'].dt.year
prescriptions_df['month'] = prescriptions_df['YEAR_MONTH'].dt.month
prescriptions_df[['medication', 'dosage']] = prescriptions_df['BNF_DESCRIPTION'].apply(extract_medication_and_dosage)

liquid_rows = prescriptions_df[prescriptions_df['medication_type'] == "liquid"]

def get_all_data(param):
    # Define the base selection of columns
    columns_to_select = [
        "year","month", "PRACTICE_NAME", "PRACTICE_CODE","POSTCODE","CHEMICAL_SUBSTANCE_BNF_DESCR","BNF_DESCRIPTION", 
        "BNF_CHAPTER_PLUS_CODE", "medication_type","medication", "dosage",
        "QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    # Define a window specification with partition_column
    window_spec_all_data = ['year', 'month', 'PRACTICE_NAME', 'CHEMICAL_SUBSTANCE_BNF_DESCR', 'BNF_DESCRIPTION']

    # Add a row number to each row based on the window specification
    base_query['row_number'] = base_query.groupby(window_spec_all_data).cumcount() + 1

    # Apply distinct if needed
    base_query = base_query.drop_duplicates(subset=columns_to_select)
    
    # Sort the DataFrame by multiple columns
    sort_columns = ["year", "month", "PRACTICE_NAME", "CHEMICAL_SUBSTANCE_BNF_DESCR", "BNF_DESCRIPTION","medication", "medication_type"]
    base_query = base_query.sort_values(by=sort_columns)

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size

    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]

    # Extracting columns and filtered data
    columns = columns_to_select
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number 

def get_bnf_descriptions(param):
    filter_columns_to_select = []
    grouping_columns_cnt = 2

    # Define the base query
    base_query = liquid_rows.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        filter_columns_to_select = filter_columns_to_select + ["medication_type"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR","BNF_DESCRIPTION"]
    
    base_query = base_query.groupby(groupby_columns).agg({   
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]

    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number 
 
def get_BNF_CHAPTER_PLUS_CODE(param):
    filter_columns_to_select = []
    grouping_columns_cnt = 1

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        filter_columns_to_select = filter_columns_to_select + ["medication_type"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication_type'].str.contains(sparam.elected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["BNF_CHAPTER_PLUS_CODE"]
    
    base_query = base_query.groupby(groupby_columns).agg({  
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number 
 
def get_MEDICATION_Name(param):
    filter_columns_to_select = []
    grouping_columns_cnt = 1

    # Define the base query
    base_query = liquid_rows.copy()
    
    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        filter_columns_to_select = filter_columns_to_select + ["medication_type"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["medication"]
    
    base_query = base_query.groupby(groupby_columns).agg({ 
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number 
 
def get_Formation(param):
    filter_columns_to_select = []
    grouping_columns_cnt = 2

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["medication","medication_type"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number

def get_CHEMICAL_SUB(param):
    
    filter_columns_to_select = []
    grouping_columns_cnt = 2

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR","medication_type"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number

def get_SurgeryPatient(param):
    prefix = "patient_count_"
    csv = glob.glob(os.path.join("filter", f"{prefix}*.csv"))
    if csv:
        # Extract modification times for each file
        file_modification_times = [(f, os.path.getmtime(f)) for f in csv]

        # Sort files by modification time and get the latest one
        latest_csv_file, _ = max(file_modification_times, key=lambda x: x[1])

        # Read the CSV file into a Pandas DataFrame
        patient_count_df = pd.read_csv(latest_csv_file)
    else:
        csv_file_path = "filter/patient_count.csv"
        patient_count_df = pd.read_csv(csv_file_path)
    
    csv_file_path_PRACTICE_CODE = 'filter/surgery.csv' 
    csv_data_PRACTICE_CODE = pd.read_csv(csv_file_path_PRACTICE_CODE)
    
    filter_columns_to_select = []
    grouping_columns_cnt = 0

    # Define the base query
    base_query = patient_count_df.copy()
    base_query = pd.merge(base_query, csv_data_PRACTICE_CODE, on=['PRACTICE_CODE'], how='inner')

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    # if selected_ChemicalSub != "":
    #     base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(selected_ChemicalSub, case=False, na=False)]
    # if selected_Medication != "":
    #     filter_columns_to_select = filter_columns_to_select + ["medication"]
    #     grouping_columns_cnt += 1
    #     base_query = base_query[base_query['medication'].str.contains(selected_Medication, case=False, na=False)]
    # if selected_Formation != "":
    #     filter_columns_to_select = filter_columns_to_select + ["medication_type"]
    #     grouping_columns_cnt += 1
    #     base_query = base_query[base_query['medication_type'].str.contains(selected_Formation, case=False, na=False)]

    base_query = base_query[["year", "month", "PRACTICE_CODE", "PRACTICE_NAME", "Patient_count"]]

    sort_columns = ["year", "month", "PRACTICE_NAME"]
    # sort_columns = ["year", "month", "PRACTICE_CODE"]
    base_query = base_query.sort_values(by=sort_columns)

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number

def get_ItemsVsPatient(param):
    
    prefix = "patient_count_"
    csv = glob.glob(os.path.join("filter", f"{prefix}*.csv"))
    if csv:
        # Extract modification times for each file
        file_modification_times = [(f, os.path.getmtime(f)) for f in csv]

        # Sort files by modification time and get the latest one
        latest_csv_file, _ = max(file_modification_times, key=lambda x: x[1])

        # Read the CSV file into a Pandas DataFrame
        patient_count_df = pd.read_csv(latest_csv_file)
    else:
        csv_file_path = "filter/patient_count.csv"
        patient_count_df = pd.read_csv(csv_file_path)
        
    # Define the base selection of columns
    columns_to_select = [
        "QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]
    filter_columns_to_select = []
    grouping_columns_cnt = 4

    # Define the base query
    base_query = prescriptions_df.copy()
    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    # if selected_ChemicalSub != "":
    #     filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
    #     grouping_columns_cnt += 1
    #     base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        filter_columns_to_select = filter_columns_to_select + ["medication_type"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["year","month","PRACTICE_NAME","PRACTICE_CODE"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        # 'QUANTITY': 'sum',
        'ITEMS': 'sum'
        # 'TOTAL_QUANTITY': 'sum',
        # 'NIC': 'sum',
        # 'ACTUAL_COST': 'sum',
    }).reset_index()
    
    base_query = pd.merge(base_query,patient_count_df, on=['PRACTICE_CODE', 'year', 'month'], how='inner')
    base_query['ITEMS_per_1000_Patient'] = (base_query['ITEMS'] / base_query['Patient_count']) * 1000

    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]
    # base_query.columns = base_query.columns + ["ITEMS_per_Patient_count_1000"]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number

def get_FormationVsPatient(param):
    
    prefix = "patient_count_"
    csv = glob.glob(os.path.join("filter", f"{prefix}*.csv"))
    if csv:
        # Extract modification times for each file
        file_modification_times = [(f, os.path.getmtime(f)) for f in csv]

        # Sort files by modification time and get the latest one
        latest_csv_file, _ = max(file_modification_times, key=lambda x: x[1])

        # Read the CSV file into a Pandas DataFrame
        patient_count_df = pd.read_csv(latest_csv_file)
    else:
        csv_file_path = "filter/patient_count.csv"
        patient_count_df = pd.read_csv(csv_file_path)
        
    filter_columns_to_select = []
    grouping_columns_cnt = 6

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["year","month","PRACTICE_NAME","PRACTICE_CODE","medication","medication_type"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        # 'QUANTITY': 'sum',
        'ITEMS': 'sum',
        # 'TOTAL_QUANTITY': 'sum',
        # 'NIC': 'sum',
        # 'ACTUAL_COST': 'sum',
    }).reset_index()
    base_query = pd.merge(base_query,patient_count_df, on=['PRACTICE_CODE', 'year', 'month'], how='inner')
    base_query['ITEMS_per_1000_Patient'] = (base_query['ITEMS'] / base_query['Patient_count']) * 1000

    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size
    
    # Apply pagination
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        result = base_query
        result.to_csv(csv_file_path, index=False)
    else:
        result = base_query.iloc[offset: offset + param.page_size]
    
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')

    return headers, columns, filtered_data , total_pages, param.page_number

def get_DosageWithFormation(param):
    filter_columns_to_select = []
    grouping_columns_cnt = 3

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "":
        base_query = base_query[base_query['medication_type'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["medication","medication_type","dosage"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        'QUANTITY': 'sum',
        'ITEMS': ['sum', 'count'],
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    
    # Flatten the MultiIndex columns
    base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]

    # Calculate the total number of records
    total_records = len(base_query)

    # Calculate the total pages
    total_pages = (total_records + param.page_size - 1) // param.page_size

    # Calculate the offset based on page_number and page_size
    offset = (param.page_number - 1) * param.page_size

    print(f"Exporting CSV: {param.exportFile}")
    
    if(param.exportFile=="csv"):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"exported_data_{current_datetime}.csv"
        csv_file_path = os.path.join(downloads_dir, csv_file_name)
        print(f"CSV File Path: {csv_file_path}")
        result = base_query
        result.to_csv(csv_file_path, index=False)
        if os.path.exists(csv_file_path):
            print("CSV file successfully created.")
        else:
            print("Error: CSV file creation failed.")
            
    # elif(param.exportFile=="pdf"):
    #     result = base_query
    #     data = [result.columns.tolist()] + result.values.tolist()
        
    #     downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
    #     current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     pdf_file_name = f"exported_data_{current_datetime}.pdf"
    #     pdf_file_path = os.path.join(downloads_dir, pdf_file_name)
        
    #     col_widths = [150, 100, 100, 100, 100, 100, 150, 100, 100]
    #     pdf = SimpleDocTemplate(pdf_file_path, pagesize=landscape(letter))

    #     # Create a table with data
    #     table = Table(data, colWidths=col_widths)

    #     # Apply styles to the table
    #     style = TableStyle([
    #         ('BACKGROUND', (0, 0), (-1, 0), '#eeeeee'),
    #         ('TEXTCOLOR', (0, 0), (-1, 0), '#333333'),
    #         ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    #         ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    #         ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
    #         ('BACKGROUND', (0, 1), (-1, -1), '#ffffff'),
    #         ('GRID', (0, 0), (-1, -1), 1, '#888888'),
    #     ])

    #     table.setStyle(style)

    #     # Build the PDF document
    #     pdf.build([table])
    else:
        result = base_query.iloc[offset: offset + param.page_size]
        
    # Extracting columns and filtered data
    columns = base_query.columns.tolist()
    headers = [col.replace('_', ' ').upper() for col in columns]
    filtered_data = result.to_dict(orient='records')
    
    return headers, columns, filtered_data , total_pages, param.page_number

# Define routes and views
@app.route('/', methods=['GET'])
def home():
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        selected_value = request.args.get('selectedValue')
        selected_year = request.args.get('selectedYear')
        selected_month = request.args.get('selectedMonth')
        selected_Surgery = request.args.get('selectedSurgery')
        selected_ChemicalSub = request.args.get('selectedChemicalSub')
        selected_Medication = request.args.get('selectedMedication')
        selected_Formation = request.args.get('selectedFormation')
        page_number = int(request.args.get('page_number', 1))
        page_size = int(request.args.get('page_size', 10))
        exportFile = request.args.get('exportFile')
        
        param = NHSParam(selected_year, selected_month,selected_Surgery, selected_ChemicalSub, selected_Medication, selected_Formation, page_number, page_size,exportFile)
        
        total_pages = 1
        columns = [] 
        filtered_data = [] 
        current_page = 1

        # Fetch dynamic columns and data based on the selected value
        if selected_value == "1" :
            headers, columns, filtered_data, total_pages, current_page = get_all_data(param)
        elif selected_value == "2" :
            headers, columns, filtered_data, total_pages, current_page = get_bnf_descriptions(param)
        elif selected_value == "3" :
            headers, columns, filtered_data, total_pages, current_page = get_BNF_CHAPTER_PLUS_CODE(param)
        elif selected_value == "4" :
            headers, columns, filtered_data, total_pages, current_page = get_MEDICATION_Name(param)
        elif selected_value == "5" :
            headers, columns, filtered_data, total_pages, current_page = get_Formation(param)
        elif selected_value == "6" :
            headers, columns, filtered_data, total_pages, current_page = get_CHEMICAL_SUB(param)
        elif selected_value == "7" :
            headers, columns, filtered_data, total_pages, current_page = get_SurgeryPatient(param)
        elif selected_value == "8" :
            headers, columns, filtered_data, total_pages, current_page = get_ItemsVsPatient(param)
        elif selected_value == "9" :
            headers, columns, filtered_data, total_pages, current_page = get_FormationVsPatient(param)
        elif selected_value == "10" :
            headers, columns, filtered_data, total_pages, current_page = get_DosageWithFormation(param)

        # Calculate prev_page_number and next_page_number
        prev_page_number = max(1, page_number - 1)
        next_page_number = min(total_pages, page_number + 1)

        # Prepare response
        response = { 'headers': headers, 'columns': columns, 'rows': filtered_data}
        
        
        all_data_param = {
            'prev_page_number': prev_page_number,
            'next_page_number': next_page_number,
            'page_number': current_page,
            'pages': total_pages
        }
        return jsonify({'response': response, 'all_data_param': all_data_param})
    else:
        return render_template('index.html')

@app.route('/extractData', methods=['GET'])
def extractData():
    result = fetch_and_process_data()
    return jsonify(result) 
if __name__ == '__main__':
    app.run(debug=True, threaded=False)
