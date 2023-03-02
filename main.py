from scraper import scrape, save
import streamlit as st
import datetime
import uuid
import pandas as pd


st.title("Kandilli Deprem Verileri")
st.subheader("(2003-Günümüz)")


# two columns
col1, col2 = st.columns(2)

with col1:
    # start year
    start_year = st.number_input("Başlangıç Yılı", min_value=2003, max_value=datetime.datetime.now().year, value=2003)

    # end year
    end_year = st.number_input("Bitiş Yılı", min_value=2003, max_value=datetime.datetime.now().year, value=datetime.datetime.now().year)

with col2:
    # start month
    start_month = st.number_input("Başlangıç Ayı", min_value=1, max_value=12, value=1)

    # end month
    end_month = st.number_input("Bitiş Ayı", min_value=1, max_value=12, value=datetime.datetime.now().month)

# select output type
output_type = st.selectbox("Dosya Formatı", ["CSV", "JSON", "Excel", "Pickle", "Parquet", "Feather"])


if st.button("Verileri Çek"):
    start = f"{start_year}{start_month if start_month >= 10 else '0' + str(start_month)}"
    end = f"{end_year}{end_month if end_month >= 10 else '0' + str(end_month)}"

    if start > end:
        st.error("Başlangıç tarihi bitiş tarihinden büyük olamaz!")
        st.stop()
    with st.spinner("Veriler çekiliyor..."):    
        df = scrape(start, end)

    st.success("Veriler çekildi!")

    unique_filename = str(uuid.uuid4())

    if output_type == "CSV":
        st.download_button(
            label="İndir",
            data=df.to_csv().encode(),
            file_name="earthquakes.csv",
            mime="text/csv"
        )

    elif output_type == "JSON":
        st.download_button(
            label="İndir",
            data=df.to_json().encode(),
            file_name="earthquakes.json",
            mime="application/json"
        )

    elif output_type == "Excel":
        # create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter("earthquakes.xlsx", engine="xlsxwriter")

        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, sheet_name="Sheet1")

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()

        with open("earthquakes.xlsx", "rb") as f:
            data = f.read()

        st.download_button(
            label="İndir",
            data=data,
            file_name="earthquakes.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


    elif output_type == "Pickle":
        save(df, "pickle")

        with open("earthquakes.pkl", "rb") as f:
            data = f.read()

        st.download_button(
            label="İndir",
            data=data,
            file_name="earthquakes.pkl",
            mime="application/octet-stream"
        )


    elif output_type == "Parquet":
        st.download_button(
            label="İndir",
            data=df.to_parquet(),
            file_name="earthquakes.parquet",
            mime="application/octet-stream"
        )

    elif output_type == "Feather":
        save(df, "feather")

        with open("earthquakes.feather", "rb") as f:
            data = f.read()

        st.download_button(
            label="İndir",
            data=data,
            file_name="earthquakes.feather",
            mime="application/octet-stream"
        )


