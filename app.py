import re
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
import sqlalchemy
import streamlit as st
import warnings
import os
import cx_Oracle
import io

# Suppress warnings
warnings.filterwarnings("ignore")

# Function to set up environment variables for Oracle client
def set_oracle_client_env(oracle_client_path):
    """Set environment variables for Oracle client."""
    os.environ["PATH"] = oracle_client_path + os.pathsep + os.environ["PATH"]
    os.environ["LD_LIBRARY_PATH"] = oracle_client_path + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["TNS_ADMIN"] = oracle_client_path  # If needed for TNS

# Function to clean strings
def clean_string(s):
    """Remove illegal characters from a string."""
    if isinstance(s, str):
        return re.sub(r'[\x00-\x1F\x7F]', '', s)
    return s

# Mock function to simulate PDF text extraction
def GetPDFText(pdfs):
    return [{"MPN": pdf, "text": "Sample text for " + pdf} for pdf in pdfs]

# Mock function for part number validation
def PN_Validation_New(pdf_data):
    validation_results = []
    for pdf in pdf_data:
        mpn = pdf['MPN']
        if 'valid' in mpn.lower():
            validation_results.append({"MPN": mpn, "STATUS": "Exact", "EQUIVALENT": "N/A", "SIMILARS": "N/A"})
        else:
            validation_results.append({"MPN": mpn, "STATUS": "Not Found", "EQUIVALENT": "N/A", "SIMILARS": "N/A"})
    return pd.DataFrame(validation_results)

# Process the uploaded Excel file for database
def process_excel_for_database(uploaded_file):
    df = pd.read_excel(uploaded_file)

    if 'MPN' not in df.columns or 'SE_MAN_NAME' not in df.columns:
        raise ValueError("The uploaded file must contain 'MPN' and 'SE_MAN_NAME' columns.")

    table_name = f'random_{uuid.uuid4().hex}'
    
    # Database connection string - Update accordingly
    engine = create_engine("oracle+cx_oracle://username:password@host:port/service_name")
    with engine.connect() as conn:
        df.to_sql(table_name, conn, if_exists='replace', chunksize=5000,
                  dtype={'MPN': sqlalchemy.types.VARCHAR(length=1024),
                         'SE_MAN_NAME': sqlalchemy.types.VARCHAR(length=1024)})

        conn.execute(text(f"ALTER TABLE {table_name} ADD NAN_MPN VARCHAR2(2048)"))
        conn.execute(text(f"UPDATE {table_name} SET NAN_MPN = CM.NONALPHANUM(MPN)"))
        conn.execute(text(f"CREATE INDEX pcntt ON {table_name}(NAN_MPN)"))

        pcn = pd.DataFrame(conn.execute(text(f"""
        SELECT {table_name}.MPN, {table_name}.SE_MAN_NAME, CM.xlp_se_manufacturer.man_name,
               cm.getpdf_url(cm.tbl_pcn_parts.PCN_ID) AS PDF
        FROM cm.tbl_pcn_parts
        JOIN CM.tbl_pcn_distinct_feature ON cm.tbl_pcn_parts.pcn_id = CM.tbl_pcn_distinct_feature.pcn_id
        JOIN {table_name} ON {table_name}.nan_mpn = cm.tbl_pcn_parts.NON_AFFECTED_PRODUCT_NAME
        JOIN CM.xlp_se_manufacturer ON cm.xlp_se_manufacturer.man_id = cm.tbl_pcn_distinct_feature.man_id
        AND cm.xlp_se_manufacturer.man_name = {table_name}.SE_MAN_NAME
        """)))

        conn.execute(text(f"DROP TABLE {table_name}"))

    return pcn

# Streamlit main function
def main():
    st.title("Main Application 🛠️")

    # Set the path for your Oracle client files
    oracle_client_path = r"C:\Users\136861\instantclient_23_5"  # Update this path

    # Set Oracle client environment variables
    set_oracle_client_env(oracle_client_path)

    # Initialize Oracle client
    cx_Oracle.init_oracle_client(lib_dir=oracle_client_path)

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
            if st.sidebar.button("Process Excel 📥"):
                try:
                    data = pd.read_excel(uploaded_file)
                    st.write("### Uploaded Data:")
                    st.dataframe(data.head())

                    if all(col in data.columns for col in ['MPN', 'SE_MAN_NAME']):
                        st.write("Processing database entries...")
                        pcn = process_excel_for_database(uploaded_file)
                        pdfs = pcn['PDF'].tolist()
                        pdf_data = GetPDFText(pdfs)
                        result_data = PN_Validation_New(pdf_data)

                        for col in ['MPN', 'STATUS', 'EQUIVALENT', 'SIMILARS']:
                            result_data[col] = result_data[col].apply(clean_string)

                        st.subheader("Validation Results")
                        status_color = {
                            'Exact': 'green',
                            'Not Found': 'red'
                        }

                        for index, row in result_data.iterrows():
                            color = status_color.get(row['STATUS'], 'black')
                            st.markdown(f"<div style='color: {color};'>{row['MPN']} - {row['STATUS']} - {row['EQUIVALENT']} - {row['SIMILARS']}</div>", unsafe_allow_html=True)

                        output = io.BytesIO()
                        result_data.to_excel(output, index=False, engine='openpyxl')
                        output.seek(0)

                        st.sidebar.download_button("Download Results 📥", 
                                                     data=output, 
                                                     file_name="PDFValidationResult.xlsx", 
                                                     mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    else:
                        st.error("The uploaded file must contain 'MPN' and 'SE_MAN_NAME' columns.")
                except Exception as e:
                    st.error(f"An error occurred while processing: {e}")

if __name__ == "__main__":
    main()
