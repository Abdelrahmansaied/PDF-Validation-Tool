import streamlit as st
import pandas as pd
import datetime
import openpyxl

def GetPDFText(pdfs):
    """Mock function to simulate PDF text extraction."""
    # Replace this with actual PDF text extraction logic
    return [{"MPN": pdf, "text": "Sample text for " + pdf} for pdf in pdfs]

def PN_Validation_New(pdf_data, mpn_column, pdf_column, data):
    """Mock function for part number validation."""
    # Replace this with actual validation logic
    validation_results = []
    for pdf in pdf_data:
        mpn = pdf['MPN']
        if 'valid' in mpn.lower():  # Just a mock condition
            validation_results.append({"MPN": mpn, "STATUS": "Exact", "EQUIVALENT": "N/A", "SIMILARS": "N/A"})
        else:
            validation_results.append({"MPN": mpn, "STATUS": "Not Found", "EQUIVALENT": "N/A", "SIMILARS": "N/A"})
    return pd.DataFrame(validation_results)

def clean_string(s):
    """Clean string function."""
    return s.strip()

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

                # Create a timestamp for the output filename
                current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"PDFValidationResult_{current_time}.xlsx"

                # Save results to Excel
                result_data.to_excel(output_file, index=False, engine='openpyxl')
                st.sidebar.download_button("Download Results 📥", data=open(output_file, "rb"), file_name=output_file)

                # Add a footer with the developer's name
                st.markdown("---")
                st.markdown("<h5 style='text-align: center;'>Developed by Sharkawy</h5>", unsafe_allow_html=True)
            else:
                st.error("The uploaded file must contain 'PDF' and 'MPN' columns.")

if __name__ == "__main__":
    main()
