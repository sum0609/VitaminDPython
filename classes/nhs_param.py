import pandas as pd
import glob
import os
class NHSParam:
    def __init__(self, selected_value, selected_year, selected_month, selected_Surgery, selected_ChemicalSub, selected_Medication, selected_Formation, page_number, page_size, exportFile=""):
        self.selected_value = selected_value
        self.selected_year = selected_year
        self.selected_month = selected_month
        self.selected_Surgery = selected_Surgery
        self.selected_ChemicalSub = selected_ChemicalSub
        self.selected_Medication = selected_Medication
        self.selected_Formation = selected_Formation
        self.page_number = page_number
        self.page_size = page_size
        self.exportFile = exportFile
        
    def getPatientCount_from_filter(self):
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
        return patient_count_df