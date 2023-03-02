import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


def scrape(start, end):
    earthquake_data = []

    # URL to scrape
    main_page_url = "http://udim.koeri.boun.edu.tr/zeqmap/hgmmap.asp"
    data_page_url = "http://udim.koeri.boun.edu.tr/zeqmap/xmlt/{}.xml"

    # Get the HTML from the URL
    r = requests.get(main_page_url)

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")

    # get the select element with the id "seldate"
    select = soup.find("select", {"id": "seldate", "name": "LBTEST"})

    # get all the options from the select element
    options = select.find_all("option")[1:]

    # filter the options
    options = [option for option in options if start <= option["value"] <= end]

    for option in options:
        # get the value of the option
        value = option["value"]

        # create the URL to scrape
        data_page_url_formatted = data_page_url.format(value)
        
        # get the XML file
        r = requests.get(data_page_url_formatted)

        # parse the XML file
        soup = BeautifulSoup(r.text, "xml", from_encoding="utf-8")

        # get the eqlist element
        eqlist = soup.find("eqlist")

        # get all the children of the eqlist element
        earthquakes = eqlist.findChildren()

        for earthquake in earthquakes:
            depth = earthquake["Depth"].strip()
            lat = earthquake["lat"].strip()
            lon = earthquake["lng"].strip()
            region = earthquake["lokasyon"].strip()
            mag = earthquake["mag"].strip()
            time = earthquake["name"].strip()

            # convert the time to a datetime object
            try:
                time = datetime.strptime(time, "%Y.%m.%d %H:%M:%S")
            except ValueError:
                time = datetime.strptime(time, "%Y.%m.%d %H.%M.%S")

            earthquake_data.append([time, lat, lon, depth, mag, region])

    # create a DataFrame from the earthquake_data
    df = pd.DataFrame(earthquake_data, columns=["Zaman", "Enlem", "Boylam", "Derinlik", "Büyüklük", "Bölge"])

    return df



def save(df, type="csv"):
    if type == "csv":
        df.to_csv("earthquakes.csv", index=False)
    elif type == "excel":
        df.to_excel("earthquakes.xlsx", index=False)
    elif type == "json":
        df.to_json("earthquakes.json", orient="records")
    elif type == "pickle":
        df.to_pickle("earthquakes.pkl")
    elif type == "parquet":
        df.to_parquet("earthquakes.parquet")
    elif type == "feather":
        df.to_feather("earthquakes.feather")

