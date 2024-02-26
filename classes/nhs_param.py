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