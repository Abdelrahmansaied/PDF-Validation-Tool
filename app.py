mport re
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
            data['STATUS'][index] = 'May be broken'
            return None
        values = pdf_data[pdf_url]

        if len(values) <= 100:
            data['STATUS'][index] = 'OCR'
            return None

        exact = ex_dif_match(re.escape(part), values) or parenthesis_part(re.escape(part), values)
        if exact:
            data['STATUS'][index] = 'Exact'
            data['EQUIVALENT'][index] = exact.group('k')
            semi_regex = semilarity(part, values)
            if semi_regex:
                data['SIMILARS'][index] = '|'.join(semi_regex)
            return None

        dif_part = '[\W_]{0,3}?'.join(sub_text(part).lower())
        dif_regex = ex_dif_match(dif_part, values)
        if dif_regex:
            data['STATUS'][index] = 'DIF_Format'
            data['EQUIVALENT'][index] = dif_regex.group('k')
            semi_regex = semilarity(part, values)
            if semi_regex:
                data['SIMILARS'][index] = '|'.join(semi_regex)
            return None

        dlb_match = dlb.get_close_matches(part, re.split('[ \n]', values), n=1, cutoff=0.65)
        if dlb_match:
            pdf_part = dlb_match[0]
            data['STATUS'][index] = (
                'Include or Missed Suffixes' 
                if sub_text(part).lower() != sub_text(pdf_part).lower() 
                else 'DIF_Format'
            )
            data['EQUIVALENT'][index] = pdf_part
            return None
        else:
            data['STATUS'][index] = 'Not Found'
            semi_match = re.search(f'(^|[ \n])(?P<k>.{repet}?{re.escape(part)}.{repet}?)($|[ \n])', values)
            if semi_match:
                data['EQUIVALENT'][index] = semi_match.group('k')

    data[['STATUS', 'EQUIVALENT', 'SIMILARS']] = None
    with ThreadPoolExecutor() as executor:
        executor.map(SET_DESC, data.index)

    return data

def GetPDFResponse(pdf):
    """Fetches a PDF file from a URL and returns its response."""
    try:
        response = requests.get(pdf, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        return pdf, io.BytesIO(response.content)
    except requests.RequestException as e:
        st.warning(f"Failed to fetch PDF: {pdf}. Error: {str(e)}")
        return pdf, None

def GetPDFText(pdfs):
    """Retrieves text from multiple PDF files."""
    pdfData = {}
    chunks = [pdfs[i:i + 100] for i in range(0, len(pdfs), 100)]
    for chunk in chunks:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(GetPDFResponse, chunk))

        for pdf, byt in results:
            if byt is not None:
                try:
                    with fitz.open(stream=byt, filetype='pdf') as doc:
                        pdfData[pdf] = '\n'.join(page.get_text() for page in doc)
                except Exception as e:
                    st.warning(f"Could not process PDF: {pdf}. Error: {str(e)}")
                    continue
    return pdfData

def main():
    # Set page title and lean response 
    st.title("PDF Validation Tool 📝")
    
    # Customize the sidebar
    st.sidebar.header("Options")
    uploaded_file = st.sidebar.file_uploader("Upload Excel file", type=["xlsx"])

    # Action button to process
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

                # Get PDF text
                pdf_data = GetPDFText(pdfs)

                # Validate part numbers
                result_data = PN_Validation_New(pdf_data, 'MPN', 'PDF', data)

                # Clean the output data
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

                # Download results
                current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"PDFValidationResult_{current_time}.xlsx"
                result_data.to_excel(output_file, index=False, engine='openpyxl')
                st.sidebar.download_button("Download Results 📥", data=open(output_file, "rb"), file_name=output_file)
            else:
                st.error("The uploaded file must contain 'PDF' and 'MPN' columns.")

if __name__ == "__main__":
    main()
