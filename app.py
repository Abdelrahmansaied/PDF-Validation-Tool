import os
import re
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
import sqlalchemy
import streamlit as st
import warnings

warnings.filterwarnings("ignore")

# Database URI
DATABASE_URI = "oracle+cx_oracle://a136861:AbdalrahmanAlsaieda136861@10.199.104.126/analytics?encoding=UTF-8" # Update with your details

# Function to clean the strings
def clean_string(s):
    """Remove illegal characters from a string."""
    if isinstance(s, str):
        return re.sub(r'[\x00-\x1F\x7F]', '', s)
    return s

# Mock function to simulate PDF text extraction
def GetPDFText(pdfs):
    """Mock function to simulate PDF text extraction."""
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

def process_excel_for_database(uploaded_file):
    """Processes the Excel file and interacts with the database."""
    df = pd.read_excel(uploaded_file)

    # Ensure the necessary columns are present
    if 'MPN' not in df.columns or 'SE_MAN_NAME' not in df.columns:
        raise ValueError("The uploaded file must contain 'MPN' and 'SE_MAN_NAME' columns.")

    # Create unique table name
    table_name = f'random_{uuid.uuid4().hex}'
    engine = create_engine(DATABASE_URI)
    
    with engine.connect() as conn:
        # Write DataFrame to SQL
        df.to_sql(table_name, conn, if_exists='replace', index=False, dtype={
            'MPN': sqlalchemy.types.VARCHAR(length=1024),
            'SE_MAN_NAME': sqlalchemy.types.VARCHAR(length=1024)
        })

        # Execute SQL commands
        conn.execute(text(f"ALTER TABLE {table_name} ADD NAN_MPN VARCHAR2(2048)"))
        conn.execute(text(f"UPDATE {table_name} SET NAN_MPN = CM.NONALPHANUM(MPN)"))
        conn.execute(text(f"CREATE INDEX pcntt ON {table_name}(NAN_MPN)"))

        # Query PDF urls
        pcn = pd.read_sql(text(f"""
        SELECT {table_name}.MPN, {table_name}.SE_MAN_NAME, CM.xlp_se_manufacturer.man_name,
               cm.getpdf_url(cm.tbl_pcn_parts.PCN_ID) AS PDF
        FROM cm.tbl_pcn_parts
        JOIN CM.tbl_pcn_distinct_feature ON cm.tbl_pcn_parts.pcn_id = CM.tbl_pcn_distinct_feature.pcn_id
        JOIN {table_name} ON {table_name}.nan_mpn = cm.tbl_pcn_parts.NON_AFFECTED_PRODUCT_NAME
        JOIN CM.xlp_se_manufacturer ON cm.xlp_se_manufacturer.man_id = cm.tbl_pcn_distinct_feature.man_id
        AND cm.xlp_se_manufacturer.man_name = {table_name}.SE_MAN_NAME
        """), conn)

        # Cleanup
        conn.execute(text(f"DROP TABLE {table_name}"))

    return pcn

def main():
    st.title("Main Application 🛠️")

    # Sidebar navigation
    st.sidebar.header("Navigation")
    selected_option = st.sidebar.selectbox("Select a Feature:", ["Main Task", "Excel Database Processing"])

    if selected_option == "Main Task":
        st.write("### Main Task Functionality")
        # Your main task functionality can be added here

    elif selected_option == "Excel Database Processing":
        st.write("### Excel Database Processing Feature 📊")

        uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

        if uploaded_file is not None:
            st.sidebar.markdown("<style>div.stButton > button:first-child { background-color: #4CAF50; color: white; }</style>", unsafe_allow_html=True)

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

                        # Clean the output data
                        for col in ['MPN', 'STATUS', 'EQUIVALENT', 'SIMILARS']:
                            result_data[col] = result_data[col].apply(clean_string)

                        # Display results
                        st.subheader("Validation Results")
                        for index, row in result_data.iterrows():
                            st.markdown(f"{row['MPN']} - {row['STATUS']} - {row['EQUIVALENT']} - {row['SIMILARS']}")

                        # Download results
                        output_file = f"PDFValidationResult.xlsx"
                        result_data.to_excel(output_file, index=False)
                        st.sidebar.download_button("Download Results 📥", data=open(output_file, "rb"), file_name=output_file)

                    else:
                        st.error("The uploaded file must contain 'MPN' and 'SE_MAN_NAME' columns.")
                except Exception as e:
                    st.error(f"An error occurred while processing: {e}")

if __name__ == "__main__":
    main()
