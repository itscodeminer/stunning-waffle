import pandas as pd

# Load your data into a DataFrame
df = pd.read_excel('your_file.xlsx')  # Replace with your actual file path

# Define the column pairs you want to compare
column_pairs = [
    ('dev_datatype', 'qa_datatype'),
    ('prod_datatype', 'qa_datatype'),
    ('staging_datatype', 'qa_datatype'),
    ('dev_datatype', 'prod_datatype'),
    ('staging_datatype', 'prod_datatype')
]

# Use pandas ExcelWriter to write the DataFrame to Excel with xlsxwriter engine
with pd.ExcelWriter('highlighted_file.xlsx', engine='xlsxwriter') as writer:
    # Write the DataFrame to the Excel file
    df.to_excel(writer, sheet_name='Sheet1', index=False)

    # Access the xlsxwriter workbook and worksheet
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    
    # Define formatting for highlighting differences
    highlight_fill = workbook.add_format({'bg_color': '#FF0000'})  # Red color for differences
    
    # Loop through each pair of columns
    for col1, col2 in column_pairs:
        # Get column indices (1-based indexing for xlsxwriter)
        col1_idx = df.columns.get_loc(col1) + 1
        col2_idx = df.columns.get_loc(col2) + 1
        
        # Apply conditional formatting for the first column of the pair
        worksheet.conditional_format(1, col1_idx - 1, len(df), col1_idx - 1, 
                                     {'type': 'formula', 
                                      'criteria': f'${col1_idx}${1} <> ${col2_idx}${1}', 
                                      'format': highlight_fill})
        
        # Apply conditional formatting for the second column of the pair
        worksheet.conditional_format(1, col2_idx - 1, len(df), col2_idx - 1, 
                                     {'type': 'formula', 
                                      'criteria': f'${col1_idx}${1} <> ${col2_idx}${1}', 
                                      'format': highlight_fill})

# The highlighted file is saved as 'highlighted_file.xlsx'
