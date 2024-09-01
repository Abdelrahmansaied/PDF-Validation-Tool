import re
import difflib as dlb
import pandas as pd
import requests
import fitz  # PyMuPDF
import io
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
import warnings

warnings.filterwarnings("ignore")

def clean_string(s):
    """Remove illegal characters from a string."""
    if isinstance(s, str):
        return re.sub(r'[\x00-\x1F\x7F]', '', s)  # Remove control characters
    return s

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
    st.title("PDF Validation Tool")

    uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

    if uploaded_file is not None:
        try:
            data = pd.read_excel(uploaded_file)
            st.write("Uploaded Data:")
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

            # Clean the output data to remove illegal characters
            for col in ['MPN', 'DECISION', 'EQUIVALENT', 'SIMILARS']:
                result_data[col] = result_data[col].apply(clean_string)

            # Show results
            st.subheader("Validation Results")
            st.dataframe(result_data[['MPN', 'DECISION', 'EQUIVALENT', 'SIMILARS']])

            # Download results
            output_file = "output_file.xlsx"
            result_data.to_excel(output_file, index=False, engine='openpyxl')
            with open(output_file, "rb") as f:
                st.download_button("Download Results", f, file_name=output_file)
        else:
            st.error("The uploaded file must contain 'PDF' and 'MPN' columns.")

if __name__ == "__main__":
    main()
