from flask import Flask, render_template,  jsonify, request, send_file
import pandas as pd
import glob
import os
from data_processing import fetch_and_process_data
from classes.nhs_param import NHSParam

from flask import Flask, render_template, send_file
import pandas as pd
from reportlab.lib.pagesizes import letter, landscape, A3, A1
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors


app = Flask(__name__)
app.secret_key = 'vitamind'

prescriptions_df = pd.DataFrame()

# TO SEPERATE MEDICINE ON THE BASIS OF ITS FORM 
# Define a UDF to categorize medication types based on keywords
def categorize_formation(description):
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

# liquid_rows = prescriptions_df[prescriptions_df['formation'] == "liquid"]

def get_all_data(param):
    
    global prescriptions_df
    # Define the base selection of columns
    columns_to_select = [
        "year","month", "PRACTICE_NAME", "PRACTICE_CODE","POSTCODE","CHEMICAL_SUBSTANCE_BNF_DESCR","BNF_DESCRIPTION", 
        "BNF_CHAPTER_PLUS_CODE", "formation","medication", "dosage",
        "QUANTITY", "ITEMS", "TOTAL_QUANTITY","NIC","ACTUAL_COST"
    ]

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "Select":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    # Define a window specification with partition_column
    window_spec_all_data = ['year', 'month', 'PRACTICE_NAME', 'CHEMICAL_SUBSTANCE_BNF_DESCR', 'BNF_DESCRIPTION']

    # Add a row number to each row based on the window specification
    base_query['row_number'] = base_query.groupby(window_spec_all_data).cumcount() + 1

    # Apply distinct if needed
    base_query = base_query.drop_duplicates(subset=columns_to_select)
    
    # Sort the DataFrame by multiple columns
    sort_columns = ["year", "month", "PRACTICE_NAME", "CHEMICAL_SUBSTANCE_BNF_DESCR", "BNF_DESCRIPTION","medication", "formation"]
    base_query = base_query.sort_values(by=sort_columns)
    return base_query, columns_to_select

def get_bnf_descriptions(param):
    
    global prescriptions_df
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
    if param.selected_Surgery != "Select":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

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
    return base_query, base_query.columns.tolist()

def get_BNF_CHAPTER_PLUS_CODE(param):
    
    global prescriptions_df
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
    if param.selected_Surgery != "Select":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

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
    return base_query, base_query.columns.tolist()
 
def get_MEDICATION_Name(param):
    
    global prescriptions_df
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
    if param.selected_Surgery != "Select":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

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
    return base_query, base_query.columns.tolist()
 
def get_Formation(param):
    
    global prescriptions_df
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
    if param.selected_Surgery != "Select":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["medication","formation"]
    
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
    return base_query, base_query.columns.tolist()

def get_CHEMICAL_SUB(param):
    
    global prescriptions_df
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
    if param.selected_Surgery != "Select":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR","formation"]
    
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
    return base_query, base_query.columns.tolist()

def get_SurgeryPatient(param):
    
    global prescriptions_df
    patient_count_df = param.getPatientCount_from_filter()
    
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
    if param.selected_Surgery != "Select":
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
    #     filter_columns_to_select = filter_columns_to_select + ["formation"]
    #     grouping_columns_cnt += 1
    #     base_query = base_query[base_query['formation'].str.contains(selected_Formation, case=False, na=False)]

    base_query = base_query[["year", "month", "PRACTICE_CODE", "PRACTICE_NAME", "Patient_count"]]

    sort_columns = ["year", "month", "PRACTICE_NAME"]
    # sort_columns = ["year", "month", "PRACTICE_CODE"]
    base_query = base_query.sort_values(by=sort_columns)
    return base_query, base_query.columns.tolist()

def get_ItemsVsPatient(param):
    
    global prescriptions_df
    patient_count_df = param.getPatientCount_from_filter()  
    
    filter_columns_to_select = []
    grouping_columns_cnt = 4

    # Define the base query
    base_query = prescriptions_df.copy()
    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "Select":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

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
    return base_query, base_query.columns.tolist()

def get_FormationVsPatient(param):
    
    global prescriptions_df
    patient_count_df = param.getPatientCount_from_filter()
    
    filter_columns_to_select = []
    grouping_columns_cnt = 6

    # Define the base query
    base_query = prescriptions_df.copy()

    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "Select":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["year","month","PRACTICE_NAME","PRACTICE_CODE","medication","formation"]
    
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
    return base_query, base_query.columns.tolist()

def get_DosageWithFormation(param):
    
    global prescriptions_df
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
    if param.selected_Surgery != "Select":
        filter_columns_to_select = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
        grouping_columns_cnt += 2
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["medication","formation","dosage"]
    
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
    return base_query, base_query.columns.tolist()

def get_Surgery(param):
    
    global prescriptions_df
    patient_count_df = param.getPatientCount_from_filter()
    
    filter_columns_to_select = []
    grouping_columns_cnt = 2

    # Define the base query
    base_query = prescriptions_df.copy()
    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        filter_columns_to_select = ["year"] + filter_columns_to_select
        grouping_columns_cnt += 1
        base_query = base_query[base_query['year'] == int(param.selected_year)]
        patient_count_df = patient_count_df[patient_count_df['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
        patient_count_df = patient_count_df[patient_count_df['month'] == int(param.selected_year)]
    if param.selected_Surgery != "Select":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["PRACTICE_NAME","PRACTICE_CODE"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        'QUANTITY': 'sum',
        'ITEMS': 'sum',
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    
    patient_count_df = patient_count_df.groupby(["PRACTICE_CODE"]).agg({
        'Patient_count': 'sum'
    }).reset_index()
    base_query = pd.merge(base_query,patient_count_df, on=['PRACTICE_CODE'], how='inner')
    
    # base_query.columns = ['_'.join(col).strip('_') for col in base_query.columns]
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]
    return base_query, base_query.columns.tolist()

def get_Surgery_year(param):
    
    global prescriptions_df
    patient_count_df = param.getPatientCount_from_filter()
    
    filter_columns_to_select = []
    grouping_columns_cnt = 3

    # Define the base query
    base_query = prescriptions_df.copy()
    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
        patient_count_df = patient_count_df[patient_count_df['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        filter_columns_to_select = filter_columns_to_select + ["month"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['month'] == int(param.selected_month)]
        patient_count_df = patient_count_df[patient_count_df['month'] == int(param.selected_year)]
    if param.selected_Surgery != "Select":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["year","PRACTICE_NAME","PRACTICE_CODE"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        'QUANTITY': 'sum',
        'ITEMS': 'sum',
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    
    patient_count_df = patient_count_df.groupby(["PRACTICE_CODE","year"]).agg({
        'Patient_count': 'sum'
    }).reset_index()
    
    base_query = pd.merge(base_query,patient_count_df, on=['PRACTICE_CODE','year'], how='inner')
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:]]
    return base_query, base_query.columns.tolist()

def get_Surgery_month(param):
    
    global prescriptions_df
    patient_count_df = param.getPatientCount_from_filter()
    
    filter_columns_to_select = []
    grouping_columns_cnt = 4

    # Define the base query
    base_query = prescriptions_df.copy()
    # Apply filters based on selected_year and selected_month
    if param.selected_year != "0":
        base_query = base_query[base_query['year'] == int(param.selected_year)]
    if param.selected_month != "0":
        base_query = base_query[base_query['month'] == int(param.selected_month)]
    if param.selected_Surgery != "Select":
        base_query = base_query[(base_query['PRACTICE_NAME'].str.contains(param.selected_Surgery, case=False, na=False)) | (base_query['PRACTICE_CODE'].str.contains(param.selected_Surgery, case=False, na=False))]
    if param.selected_ChemicalSub != "Select":
        filter_columns_to_select = filter_columns_to_select + ["CHEMICAL_SUBSTANCE_BNF_DESCR"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['CHEMICAL_SUBSTANCE_BNF_DESCR'].str.contains(param.selected_ChemicalSub, case=False, na=False)]
    if param.selected_Medication != "Select":
        filter_columns_to_select = filter_columns_to_select + ["medication"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['medication'].str.contains(param.selected_Medication, case=False, na=False)]
    if param.selected_Formation != "Select":
        filter_columns_to_select = filter_columns_to_select + ["formation"]
        grouping_columns_cnt += 1
        base_query = base_query[base_query['formation'].str.contains(param.selected_Formation, case=False, na=False)]

    groupby_columns = filter_columns_to_select + ["year","month","PRACTICE_NAME","PRACTICE_CODE"]
    
    base_query = base_query.groupby(groupby_columns).agg({
        'QUANTITY': 'sum',
        'ITEMS': 'sum',
        'TOTAL_QUANTITY': 'sum',
        'NIC': 'sum',
        'ACTUAL_COST': 'sum',
    }).reset_index()
    
    base_query = pd.merge(base_query,patient_count_df, on=['PRACTICE_CODE', 'year', 'month'], how='inner')
    
    # Rename the columns
    base_query.columns = groupby_columns + [f'{col}' for col in base_query.columns[grouping_columns_cnt:] ]
    return base_query, base_query.columns.tolist()

# Define routes and views
@app.route('/', methods=['GET'])
def home():
    global prescriptions_df
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
        
        param = NHSParam(selected_value, selected_year, selected_month,selected_Surgery, selected_ChemicalSub, selected_Medication, selected_Formation, page_number, page_size,exportFile)
        
        total_pages = 1
        columns = [] 
        filtered_data = [] 
        current_page = page_number
            
        base_query, columns_to_select = chooseFunction(param)

        # Calculate the total number of records
        total_records = len(base_query)

        # Calculate the total pages
        total_pages = (total_records + param.page_size - 1) // param.page_size

        # Calculate the offset based on page_number and page_size
        offset = (param.page_number - 1) * param.page_size

        # #eporting file to csv/pdf
        # result = param.exportFileCsvPdf(base_query,offset)
        result = base_query.iloc[offset: offset + param.page_size]
        # Extracting columns and filtered data
        columns = columns_to_select
        headers = [col.replace('_', ' ').upper() for col in columns]
        filtered_data = result.to_dict(orient='records')
        
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
            
        
        prescriptions_df['formation'] = prescriptions_df['BNF_DESCRIPTION'].apply(categorize_formation)
        prescriptions_df['YEAR_MONTH'] = pd.to_datetime(prescriptions_df['YEAR_MONTH'], format='%Y%m')
        prescriptions_df['year'] = prescriptions_df['YEAR_MONTH'].dt.year
        prescriptions_df['month'] = prescriptions_df['YEAR_MONTH'].dt.month
        prescriptions_df[['medication', 'dosage']] = prescriptions_df['BNF_DESCRIPTION'].apply(extract_medication_and_dosage)

        csv_file_path_BNF_DESCR = 'filter/vitamind.csv' 
        csv_data_BNF_DESCR = pd.read_csv(csv_file_path_BNF_DESCR)
        CHEMICAL_SUBSTANCE_BNF_DESCR = csv_data_BNF_DESCR['CHEMICAL_SUBSTANCE_BNF_DESCR'].astype(str).unique()
        CHEMICAL_SUBSTANCE_BNF_DESCR.sort()
        medications = prescriptions_df['medication'].unique()
        medications.sort()
        
        csv_file_path_PRACTICE_CODE = 'filter/surgery.csv' 
        csv_data_PRACTICE_CODE = pd.read_csv(csv_file_path_PRACTICE_CODE)
        practice_name_code_mapping = dict(zip(csv_data_PRACTICE_CODE['PRACTICE_NAME'], csv_data_PRACTICE_CODE['PRACTICE_CODE']))
        PRACTICE_NAMES = sorted(csv_data_PRACTICE_CODE['PRACTICE_NAME'])

        return render_template('index.html', medications=medications, CHEMICAL_SUBSTANCE_BNF_DESCR=CHEMICAL_SUBSTANCE_BNF_DESCR, PRACTICE_NAMES=PRACTICE_NAMES, practice_name_code_mapping=practice_name_code_mapping )

def chooseFunction(param):
    if param.selected_value == "1" :
        base_query, columns_to_select = get_all_data(param)
    elif param.selected_value == "2" :
        base_query, columns_to_select = get_bnf_descriptions(param)
    elif param.selected_value == "3" :
        base_query, columns_to_select = get_BNF_CHAPTER_PLUS_CODE(param)
    elif param.selected_value == "4" :
        base_query, columns_to_select = get_MEDICATION_Name(param)
    elif param.selected_value == "5" :
        base_query, columns_to_select = get_Formation(param)
    elif param.selected_value == "6" :
        base_query, columns_to_select = get_CHEMICAL_SUB(param)
    elif param.selected_value == "7" :
        base_query, columns_to_select = get_SurgeryPatient(param)
    elif param.selected_value == "8" :
        base_query, columns_to_select = get_ItemsVsPatient(param)
    elif param.selected_value == "9" :
        base_query, columns_to_select = get_FormationVsPatient(param)
    elif param.selected_value == "10" :
        base_query, columns_to_select = get_DosageWithFormation(param)
    elif param.selected_value == "11" :
        base_query, columns_to_select = get_Surgery(param)
    elif param.selected_value == "12" :
        base_query, columns_to_select = get_Surgery_year(param)
    elif param.selected_value == "13" :
        base_query, columns_to_select = get_Surgery_month(param)
    return base_query, columns_to_select

@app.route('/extractData', methods=['GET'])
def extractData():
    result = fetch_and_process_data()
    return jsonify(result) 

@app.route('/download_csv_pdf')
def download_csv_pdf():
    
    print('download_csv_pdf called')
    # if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
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
    
    param = NHSParam(selected_value, selected_year, selected_month,selected_Surgery, selected_ChemicalSub, selected_Medication, selected_Formation, page_number, page_size,exportFile)
    if(exportFile=='csv'):
        return download_csv(param)
    else:
        return download_pdf(param)
    
def download_csv(param):
    print('download_csv called')
    # Generate CSV file
    csv_data = generate_csv(param)

    # Save CSV file on the server
    csv_filename = 'prescriptions.csv'
    with open(csv_filename, 'w', newline='') as csv_file:
        csv_file.write(csv_data)

    # Return the CSV file to the client for download
    return send_file(csv_filename, as_attachment=True)

def download_pdf(param):
    # Generate and save PDF file
    generate_pdf(param)

    # Return the PDF file to the client for download
    return send_file('prescriptions.pdf', as_attachment=True)

def generate_csv(param):
    print('generate_csv called')
    base_query, columns_to_select = chooseFunction(param)
    selected_rows = base_query
    csv_string = selected_rows.to_csv(index=False)
    return csv_string

def generate_pdf(param):
    base_query, columns_to_select = chooseFunction(param)
    selected_rows = base_query
    pdf_file_path = 'prescriptions.pdf'

    column_count = selected_rows.shape[1]
    pdf_size = (
        landscape(A1) if column_count >= 11 else
        landscape(A3) if column_count >= 8 else
        landscape(letter) if column_count == 7 else
        letter
    )

    pdf = SimpleDocTemplate(pdf_file_path, pagesize=pdf_size)
    
    data = [selected_rows.columns.tolist()] + selected_rows.values.tolist()
    table = Table(data, repeatRows=1)
    
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ])
    
    table.setStyle(style)

    pdf.build([table])

if __name__ == '__main__':
    app.run(debug=True, threaded=False)
