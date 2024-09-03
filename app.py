import re
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
import sqlalchemy
import streamlit as st
import warnings

warnings.filterwarnings("ignore")

# Function to clean the strings
def clean_string(s):
    """Remove illegal characters from a string."""
    if isinstance(s, str):
        return re.sub(r'[\x00-\x1F\x7F]', '', s)
    return s

# Mock function to simulate PDF text extraction
def PN_Validation_New(pdf_data, part_col, pdf_col, data):
    sub_text = lambda x: re.sub('[\W_]', '', x)
    repet = '{0,20}'
    spa = "[^\w#*]{0,2}?"
    ex_dif_match = lambda x, values: re.search(f'(^|[\n ]{spa})(?P<k>{x})({spa}[\n ]|$)', values, flags=re.IGNORECASE)
    parenthesis_part = lambda x, values: re.search(f'(^|[\n ]{spa})$(?P<k>{x})$({spa}[\n ]|$)', values, flags=re.IGNORECASE)
    
    def semilarity(part, values):
        return {
            match.group('key').strip() + match.group('v').strip()
            for match in re.finditer(
                f'(^|(?<=[\n ]))(?P<key>[^\n ]{repet}?{re.escape(part)})(?P<v>[\w\-\+\*$$\.,\/]{repet}?[\W]?)(?=[\n ]|$)',
                values,
                flags=re.IGNORECASE
            )
        }

    def SET_DESC(index):
        part = data[part_col][index]
        pdf_url = data[pdf_col][index]
        if pdf_url not in pdf_data:
            data['DECISION'][index] = 'Invalid PDF'
            return None
        values = pdf_data[pdf_url]
        
        if len(values) <= 100:
            data['DECISION'][index] = 'OCR'
            return None

        exact = ex_dif_match(re.escape(part), values) or parenthesis_part(re.escape(part), values)
        if exact:
            data['DECISION'][index] = 'Exact'
            data['EQUIVALENT'][index] = exact.group('k')
            semi_regex = semilarity(part, values)
            if semi_regex:
                data['SIMILARS'][index] = '|'.join(semi_regex)
            return None
        
        dif_part = '[\W_]{0,3}?'.join(sub_text(part).lower())
        dif_regex = ex_dif_match(dif_part, values)
        if dif_regex:
            data['DECISION'][index] = 'DIF_Format'
            data['EQUIVALENT'][index] = dif_regex.group('k')
            semi_regex = semilarity(part, values)
            if semi_regex:
                data['SIMILARS'][index] = '|'.join(semi_regex)
            return None
        
        dlb_match = dlb.get_close_matches(part, re.split('[ \n]', values), n=1, cutoff=0.65)
        if dlb_match:
            pdf_part = dlb_match[0]
            data['DECISION'][index] = 'Contains +/–' if sub_text(part).lower() != sub_text(pdf_part).lower() else 'DIF_Format'
            data['EQUIVALENT'][index] = pdf_part
            return None
        else:
            data['DECISION'][index] = 'Need Check'
            semi_match = re.search(f'(^|[ \n])(?P<k>.{repet}?{re.escape(part)}.{repet}?)($|[ \n])', values)
            if semi_match:
                data['EQUIVALENT'][index] = semi_match.group('k')

    data[['DECISION', 'EQUIVALENT', 'SIMILARS']] = None
    with ThreadPoolExecutor() as executor:
        executor.map(SET_DESC, data.index)

    return data

def GetPDFResponse(pdf):
    try:
        response = requests.get(pdf, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        return pdf, io.BytesIO(response.content)
    except Exception as e:
        return pdf, None

def GetPDFText(pdfs):
    pdfData = {}
    chunks = [pdfs[i:i + 100] for i in range(0, len(pdfs), 100)]  # Adjust chunk size if needed
    for chunk in chunks:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(GetPDFResponse, chunk))
        
        for pdf, byt in results:
            if byt is not None:  # Only process if byt is valid
                try:
                    with fitz.open(stream=byt, filetype='pdf') as doc:
                        pdfData[pdf] = '\n'.join(page.get_text() for page in doc)
                except Exception as e:
                    continue
    return pdfData

def process_excel_for_database(uploaded_file):
    """Processes the Excel file and interacts with the database."""
    df = pd.read_excel(uploaded_file)

    # Ensure the necessary columns are present
    if 'MPN' not in df.columns or 'SE_MAN_NAME' not in df.columns:
        raise ValueError("The uploaded file must contain 'MPN' and 'SE_MAN_NAME' columns.")

    # Create unique table name
    table_name = f'random_{uuid.uuid4().hex}'
    
    # Database connection string
    engine = create_engine("oracle+cx_oracle://a136861:AbdalrahmanAlsaieda136861@10.199.104.126/analytics?encoding=UTF-8")
    conn2 = engine.connect()

    # Write DataFrame to SQL
    df.to_sql(table_name, engine, if_exists='replace', chunksize=5000,
              method=None, dtype={'MPN': sqlalchemy.types.VARCHAR(length=1024),
                                  'SE_MAN_NAME': sqlalchemy.types.VARCHAR(length=1024)})

    # Execute SQL commands
    conn2.execute(text(f"ALTER TABLE {table_name} ADD NAN_MPN VARCHAR2(2048)"))
    conn2.execute(text(f"UPDATE {table_name} SET NAN_MPN = CM.NONALPHANUM(MPN)"))
    conn2.execute(text(f"CREATE INDEX pcntt ON {table_name}(NAN_MPN)"))

    # Query PDF urls
    pcn = pd.DataFrame(conn2.execute(text(f"""
    SELECT {table_name}.MPN, {table_name}.SE_MAN_NAME, CM.xlp_se_manufacturer.man_name,
           cm.getpdf_url(cm.tbl_pcn_parts.PCN_ID) AS PDF
    FROM cm.tbl_pcn_parts
    JOIN CM.tbl_pcn_distinct_feature ON cm.tbl_pcn_parts.pcn_id = CM.tbl_pcn_distinct_feature.pcn_id
    JOIN {table_name} ON {table_name}.nan_mpn = cm.tbl_pcn_parts.NON_AFFECTED_PRODUCT_NAME
    JOIN CM.xlp_se_manufacturer ON cm.xlp_se_manufacturer.man_id = cm.tbl_pcn_distinct_feature.man_id
    AND cm.xlp_se_manufacturer.man_name = {table_name}.SE_MAN_NAME
    """)))

    # Cleanup
    conn2.execute(text(f"DROP TABLE {table_name}"))

    return pcn

def main():
    st.title("Main Application 🛠️")

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
                        pdfs = pcn['pdf'].tolist()
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
