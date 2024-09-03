import os
import re
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
import sqlalchemy
import streamlit as st
import requests
import zipfile
import io
import warnings

warnings.filterwarnings("ignore")

# Function to download a ZIP file from a given URL
def download_zip(url):
    response = requests.get(url)
    if response.status_code == 200:
        # Check the content type
        if 'zip' in response.headers.get('Content-Type', ''):
            return response.content
        else:
            st.error("The URL does not point to a valid ZIP file.")
    else:
        st.error(f"Failed to download ZIP file: {response.status_code}")
    return None

# Function to extract the ZIP file
def extract_zip(content, extract_to):
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
            zip_file.extractall(extract_to)
            return True
    except zipfile.BadZipFile:
        st.error("The downloaded file is not a valid ZIP file.")
        return False

# Function to download and extract Instant Client DLL files
def download_instant_client():
    zip_url = 'https://drive-data-export-eu.usercontent.google.com/download/15nci6fofda8jc487dsos3shspejcvpf/40dao7onbafnug58nutaktuuqfpigjbp/1725349500000/78bffaa5-a2a9-41fd-a37d-57848fe4a35b/100060732374272085211/ADt3v-PW8kSkwo4Cn7A76oOnzBuLpRjCeJn9RJg3hXY-zlHatyERxUEmv2SxsZ_SJ5d9PuD3VvDhGaz3kQUbXYQ4enQWx0ZoD5gN11C9LuS2BVgztKjUqA1vschJ87aM2F_PDz2J8uCmY53yRY4J44scsW40vHBGTSBLjptDZ8nLFG5RT7BNyaN7k0hFUK9_NTfDM8TVb6SkQRK1n441TXNiuHOpzqVuN8K4qSZnrFxKwg3hZPA_X742nas-VItfO4ZRYgkTMIbDDQqhFezvnf2g8fQEkQKPjqDVpNJv1fha2KQY4PlQFiNtOpeqwCndsdnBO2tCWt9m?j=78bffaa5-a2a9-41fd-a37d-57848fe4a35b&user=947978676346&i=0&authuser=0'

    extract_to = 'instantclient'
    os.makedirs(extract_to, exist_ok=True)

    st.write("Downloading Instant Client ZIP file...")
    zip_content = download_zip(zip_url)

    if zip_content:
        st.success("Downloaded ZIP file. Extracting...")
        if extract_zip(zip_content, extract_to):
            st.success("Extraction completed.")
    else:
        st.error("Failed to download the ZIP file.")

# Other functions remain unchanged...

def main():
    st.title("Main Application 🛠️")

    # Download Instant Client files
    download_instant_client()

    # Sidebar navigation
    st.sidebar.header("Navigation")
    selected_option = st.sidebar.selectbox("Select a Feature:", ["Main Task", "Excel Database Processing"])

    if selected_option == "Main Task":
        st.write("### Main Task Functionality")
        # Your main task functionality goes here

    elif selected_option == "Excel Database Processing":
        st.write("### Excel Database Processing Feature 📊")

        uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

        if uploaded_file is not None:
            st.sidebar.markdown("<style>div.stButton > button:first-child { background-color: #4CAF50; color: white; }</style>", unsafe_allow_html=True)

            if st.sidebar.button("Process Excel 📥"):
                try:
                    # Load data for database processing
                    data = pd.read_excel(uploaded_file)
                    st.write("### Uploaded Data:")
                    st.dataframe(data.head())
                except Exception as e:
                    st.error(f"Error reading the Excel file: {e}")
                    return
                
                # Handling the case for the validation with MPN and SE_MAN_NAME
                if all(col in data.columns for col in ['MPN', 'SE_MAN_NAME']):
                    st.write("Processing database entries...")
                    
                    try:
                        pcn = process_excel_for_database(uploaded_file)
                        pdfs = pcn['PDF'].tolist()  # Ensure the column name matches
                        pdf_data = GetPDFText(pdfs)
                        result_data = PN_Validation_New(pdf_data, 'mpn', 'pdf', pcn)

                        # Clean the output data
                        for col in ['MPN', 'STATUS', 'EQUIVALENT', 'SIMILARS']:
                            result_data[col] = result_data[col].apply(clean_string)

                        # Display results with colors
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

                        # Download results
                        output_file = f"PDFValidationResult.xlsx"
                        result_data.to_excel(output_file, index=False, engine='openpyxl')
                        st.sidebar.download_button("Download Results 📥", data=open(output_file, "rb"), file_name=output_file)

                    except Exception as e:
                        st.error(f"An error occurred while processing: {e}")
                else:
                    st.error("The uploaded file must contain 'MPN' and 'SE_MAN_NAME' columns.")

if __name__ == "__main__":
    main()
