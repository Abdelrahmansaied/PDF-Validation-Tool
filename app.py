import re
import difflib as dlb
import datetime
import pandas as pd
import requests
import fitz  # PyMuPDF
import io
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import warnings

warnings.filterwarnings("ignore")

# Function to clean the strings
def clean_string(s):
    """Remove illegal characters from a string."""
    if isinstance(s, str):
        return re.sub(r'[\x00-\x1F\x7F]', '', s)
    return s

# Validation function
def PN_Validation_New(pdf_data, part_col, pdf_col, data):
    # Your existing function code...
    # (No changes made here for brevity)
    pass

def GetPDFResponse(pdf):
    """Fetches a PDF file from a URL and returns its response."""
    # Your existing function code...
    pass

def GetPDFText(pdfs):
    """Retrieves text from multiple PDF files."""
    # Your existing function code...
    pass

def main():
    st.title("PDF Validation Tool 📝")
    
    st.sidebar.header("Options")
    uploaded_file = st.sidebar.file_uploader("Upload Excel file", type=["xlsx"])

    if uploaded_file is not None:
        st.sidebar.markdown("<style>div.stButton > button:first-child { background-color: #4CAF50; color: white; }</style>", unsafe_allow_html=True)
        if st.sidebar.button("Process PDFs 🔍"):
            try:
                data = pd.read_excel(uploaded_file)
                st.write("### Uploaded Data:")
                st.dataframe(data.head())
            except Exception as e:
                st.error(f"Error reading the Excel file: {e}")
                return

            if 'PDF' in data.columns and 'MPN' in data.columns:
                pdfs = data['PDF'].tolist()
                st.write("Processing PDFs...")

                pdf_data = GetPDFText(pdfs)
                result_data = PN_Validation_New(pdf_data, 'MPN', 'PDF', data)

                for col in ['MPN', 'STATUS', 'EQUIVALENT', 'SIMILARS']:
                    result_data[col] = result_data[col].apply(clean_string)

                # Show results with colors
                st.subheader("Validation Results")
                status_color = {
                    'Exact': 'green',
                    'DIF_Format': '#FFA500',
                    'Include or Missed Suffixes': 'orange',
                    'Not Found': 'red',
                    'May be broken': 'grey'
                }
                
                for index, row in result_data.iterrows():
                    color = status_color.get(row['STATUS'], 'black')
                    st.markdown(f"<div style='color: {color};'>{row['MPN']} - {row['STATUS']} - {row['EQUIVALENT']} - {row['SIMILARS']}</div>", unsafe_allow_html=True)

                # Prepare styled output file
                current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"PDFValidationResult_{current_time}.xlsx"

                # Create styled DataFrame
                styled_result = (result_data.style
                    .apply(lambda x: ['background-color: powderblue' if i % 2 == 0 else '' for i in range(len(x))], axis=0)  # Alternate row color
                    .set_table_styles({
                        'MPN': [{'selector': 'th', 'props': [('background-color', 'steelblue'), ('color', 'white')]}]},
                        axis=None
                    )
                )

                # Save to Excel with styles
                with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                    styled_result.to_excel(writer, sheet_name='Validation Results', index=False)
                
                st.sidebar.download_button("Download Results 📥", data=open(output_file, "rb"), file_name=output_file)
            else:
                st.error("The uploaded file must contain 'PDF' and 'MPN' columns.")

if __name__ == "__main__":
    main()
