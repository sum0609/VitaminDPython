import os
from datetime import datetime
from reportlab.lib.pagesizes import A1, A3,letter,landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors
class NHSParam:
    def __init__(self, selected_year, selected_month, selected_Surgery, selected_ChemicalSub, selected_Medication, selected_Formation, page_number, page_size, exportFile=""):
        self.selected_year = selected_year
        self.selected_month = selected_month
        self.selected_Surgery = selected_Surgery
        self.selected_ChemicalSub = selected_ChemicalSub
        self.selected_Medication = selected_Medication
        self.selected_Formation = selected_Formation
        self.page_number = page_number
        self.page_size = page_size
        self.exportFile = exportFile
        
    def exportFileCsvPdf(self,base_query,offset):
        if(self.exportFile=="csv"):
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
                
        elif(self.exportFile=="pdf"):
            result = base_query

            downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            pdf_file_name = f"exported_data_{current_datetime}.pdf"
            pdf_file_path = os.path.join(downloads_dir, pdf_file_name)
            
            column_count = result.shape[1]
            pdf_size = landscape(A1) if column_count >=11 else (landscape(A3) if column_count >= 8 else (landscape(letter) if column_count == 7 else letter))

            pdf = SimpleDocTemplate(pdf_file_path, pagesize=pdf_size)
            data = [result.columns.tolist()] + result.values.tolist()
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
        else:
            result = base_query.iloc[offset: offset + self.page_size]
        return result
            