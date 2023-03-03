import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import multiprocessing as mp


# URLs to scrape
MAIN_URL = "http://udim.koeri.boun.edu.tr/zeqmap/hgmmap.asp"
DATA_URL = "http://udim.koeri.boun.edu.tr/zeqmap/xmlt/{}.xml"

def process_option(option):
        # create the URL to scrape
        data_page_url_formatted = DATA_URL.format(option)
        
        # get the XML file
        r = requests.get(data_page_url_formatted)

        # parse the XML file
        soup = BeautifulSoup(r.text, "xml")

        # get the eqlist element
        eqlist = soup.find("eqlist")

        # get all the children of the eqlist element
        earthquakes = eqlist.findChildren()

        results = []
        for earthquake in earthquakes:
            try:
                depth = float(earthquake["Depth"].strip())
                lat = float(earthquake["lat"].strip())
                lon = float(earthquake["lng"].strip())
                region = earthquake["lokasyon"].strip()
                mag = float(earthquake["mag"].strip())
                time = earthquake["name"].strip()
            except (KeyError, ValueError):
                continue


            # convert the time to a datetime object
            try:
                time = datetime.strptime(time, "%Y.%m.%d %H:%M:%S")
            except ValueError:
                time = datetime.strptime(time, "%Y.%m.%d %H.%M.%S")
            
            # append the data to the list
            results.append({
                "Zaman": time,
                "Enlem": lat,
                "Boylam": lon,
                "Derinlik": depth,
                "Büyüklük": mag,
                "Bölge": region,
            })

        # return the list
        return results


def scrape(start, end):
    earthquake_data = pd.DataFrame()

    # Get the HTML from the URL
    r = requests.get(MAIN_URL)

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")

    # get the select element with the id "seldate"
    select = soup.find("select", {"id": "seldate", "name": "LBTEST"})

    # get all the options from the select element
    options = select.find_all("option")[1:]

    # filter the options
    options = [option["value"] for option in options if start <= option["value"] <= end]

    # create a pool of processes
    pool = mp.Pool(mp.cpu_count())

    # process the options
    results = pool.map(process_option, options)

    # close the pool
    pool.close()

    # flatten the list
    results = [item for sublist in results for item in sublist]

    # create a dataframe from the list
    earthquake_data = pd.DataFrame(results)

    # sort the dataframe by time
    earthquake_data = earthquake_data.sort_values("Zaman", ascending=False).reset_index(drop=True)

    # return the dataframe
    return earthquake_data



def save(df, type="csv"):
    if type == "csv":
        df.to_csv("earthquakes.csv", index=False)
    elif type == "excel":
        writer = pd.ExcelWriter("earthquakes.xlsx", engine="xlsxwriter")
        df.to_excel(writer, sheet_name="Sheet1", index=False)
    elif type == "json":
        df.to_json("earthquakes.json", orient="records")
    elif type == "pickle":
        df.to_pickle("earthquakes.pkl")
    elif type == "parquet":
        df.to_parquet("earthquakes.parquet")
    elif type == "feather":
        df.to_feather("earthquakes.feather")



if __name__ == "__main__":
    start = "200301"
    end = "202303"

    df = scrape(start, end)

    save(df, "csv")

