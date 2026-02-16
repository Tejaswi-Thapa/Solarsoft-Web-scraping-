# Steps I need to take/took:
# 1. Downloaded the website with requests.get()
# 2. Made the HTML readable with BeautifulSoup
# 3. Found all "Snapshot Time" links in the base table
# 4. Visited each snapshot link (each data has a link)
# 5. Scarped the nested event table for each snapshot, extracting EName, GOES Class, Derived Position, etc.
# 6. Stored all event rows in a list and converted them into a DataFrame
# 7. Converted date/time columns into datetime format
# 8. Combined old CSV data with newly scraped data
# 9. Removed overlapping events across snapshots by deduplicating on EName, keeping the most recent snapshot's data
# 10. Sorted the DateFrame so the newest events are kept
# 11. Saved the dataframe to a CSV file


# python treats annotations as strings by default, so this is just for clarity and future compatibility
from __future__ import annotations

# bs4 = Beautiful Soup 4
# Is a python library used to: read HTML, understand the structure of a webpage and extract specific data from it. It makes web scraping easier by providing tools to navigate and search the HTML content of a webpage.
from bs4 import BeautifulSoup

# imports a function that combines a base URL with a relative URL to create an absolute URL. This is useful for handling links on web pages that may be relative (e.g., "/page.html") rather than absolute (e.g., "https://example.com/page.html"). By using urljoin, you can ensure that you are working with complete URLs when scraping data from web pages.
from urllib.parse import urljoin

# import os for file handling and path operations
import os

# Gives access to system-level features like command line arguments and exiting the program
import sys

# imports the pandas library (data tool), and renames it to pd for easier usage in the code
# pandas is used for tables of data (Data Frame), Cleaning data, converting columns to dates/numbers, and writing/reading CSV files
import pandas as pd
# NumPy is mainly about math and numerical data
# imports the requests library used to make HTTP requests (download webpages, call APIs).

# Requests = Python's way to download a webpage.
import requests


# creating variables: "URL" "MIN_YEAR" and "CSV_PATH"
# Instead of writing the URL over and over again, I am able to just write requests.get(URL)

BASE_URL = "https://lmsal.com/solarsoft/latest_events_archive.html"
# used 2020 to avoid scraping too much data, and to focus on recent events. This can be changed to any year you want to scrape from.
MIN_YEAR = 2020

EVENTS_CSV_PATH = "lmsal_events.csv"
SUMMARY_CSV_PATH = "lmsal_latest_events_archive.csv"  # optional


# DataFrame: A table like Excel and Google Sheets, but in Python. It has rows and columns, and you can do all sorts of things with it (filter, sort, calculate, etc.). We will use it to store the scraped data and then save it to a CSV file.
# df = pd.DataFrame(rows) -> Creates a DataFrame from the list of rows we scraped and Store it in a variable called df. Each row is a list of values corresponding to the columns we defined.
# def = define. Used to create a function. | "fetch_archive_table" is the name of the function. | "(url: str)" means the function takes one argument called "url" which should be a string. | "-> pd.DataFrame" means the function will return a pandas DataFrame.

def get_soup(url: str) -> BeautifulSoup | None:  # Line Explaination: This function is called "get_soup". It takes a URL as input (a string) and returns either a BeautifulSoup object (which represents the parsed HTML of the webpage) or None if there was an error (like a 404 not found). The function uses the requests library to download the webpage, checks for errors, and then parses the HTML with BeautifulSoup. If it encounters any issues during the request, it will return None instead of crashing the program.
    """Download a URL and return BeautifulSoup, or None if request fails/404s."""  # This is a docstring, which is a comment that explains what the function does. It says that this function will download a webpage from the given URL and return a BeautifulSoup object if successful, or None if the request fails (like if the page doesn't exist).
    headers = {"User-Agent": "Mozilla/5.0"}  # Why you need it: When you scrape a website, servers sometimes block: bots, scripts, and non browser requests. A user agent tells the server "Hey, I am a browser, not a bot". This can help you avoid getting blocked and get the data you want. Modzilla/5.0 is a common user agent string that mimics a web browser, making it more likely that the server will allow the request.
    try:
        # This line tries to download the webpage at the given URL using the requests library. It includes the custom headers we defined (to mimic a browser) and sets a timeout of 30 seconds to prevent hanging if the server is slow to respond.
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:  # If the server responds with a 404 status code, it means the page was not found. In this case, we want to return None to indicate that we couldn't get the data from that URL.
            # If the request was successful (not a 404), we check for any other HTTP errors with resp.raise_for_status(). If there are no errors, we parse the HTML content of the response using BeautifulSoup and return the resulting object. This allows us to easily navigate and extract data from the webpage later on.
            return None
        resp.raise_for_status()  # This line checks if the HTTP request was successful. If the server returned an error status code (like 500 or 403), this will raise an exception, which we catch in the except block. If the request was successful, we proceed to parse the HTML.
        # If the request was successful, we take the text content of the response (the HTML of the webpage) and parse it with BeautifulSoup using the "html.parser". This allows us to work with the webpage's structure and extract data from it. We then return the BeautifulSoup object for further processing.
        return BeautifulSoup(resp.text, "html.parser")
    # If there was any error during the request (like a timeout, connection error, or HTTP error), we catch the exception here. Instead of crashing the program, we simply return None to indicate that we couldn't retrieve the webpage.
    except requests.exceptions.RequestException:
        # This way, the rest of our code can check if the result is None and handle it gracefully (like skipping that URL or logging an error) without crashing the entire scraping process.
        return None


# Defines another function called "load_existing" that takes a string argument "csv_path" (the path to the CSV file) and returns a pandas DataFrame. This function is responsible for loading the existing data from the CSV file if it exists, or returning an empty DataFrame if the file doesn't exist or is empty.
def load_existing(csv_path: str) -> pd.DataFrame:
    """Load existing CSV if it exists; otherwise return empty DataFrame."""
 # checks if the file at "csv_path" does not exist (os.path.exists(csv_path) returns False) or if the file exists but is empty (os.path.getsize(csv_path) == 0). If either of these conditions is true, it means there is no existing data to load.
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        # If the file doesn't exist or is empty, we return an empty DataFrame. This allows the rest of the code to work without errors, even if there is no existing CSV file.
        return pd.DataFrame()
# this line reads the existing CSV file into a DataFrame using pandas. The argument dtype=str ensures that all columns are read as strings, which can help prevent issues with data types when we later combine this DataFrame with the new data we scrape.
    return pd.read_csv(csv_path, dtype=str)


# This function is called "fetch_archive_summary_table". It takes a URL as input (a string) and returns a pandas DataFrame. The purpose of this function is to scrape the summary table from the given URL, extract relevant data, and return it in a structured format as a DataFrame.
def fetch_archive_summary_table(url: str) -> pd.DataFrame:
    # This line calls the get_soup function we defined earlier to download the webpage at the given URL and parse it with BeautifulSoup. The resulting BeautifulSoup object is stored in the variable "soup". If there was an error during the request (like a 404), "soup" will be None.
    soup = get_soup(url)
    # If we couldn't get the soup (like if the page was not found), we return an empty DataFrame. This allows the rest of the code to handle this case gracefully without crashing.
    if soup is None:
        return pd.DataFrame()  # If the soup was successfully retrieved, we proceed to find the summary table in the HTML. We look for all "tr" (table row) elements in the soup, and for each row, we extract the "td" (table data) elements. We check if there are at least 9 columns (since we expect 9 pieces of data based on the columns we want). If there are enough columns, we extract the text from the first 9 columns, strip any extra whitespace, and store it as a list in "row_data". We then append this list to our "rows" list, which will eventually contain all the data from the summary table.

    # We initialize an empty list called "rows" to store the data we extract from each row of the summary table. Each entry in this list will be a list of values corresponding to the columns we want to capture (like Report Date, Start Time, End Time, etc.). After we finish extracting all the rows, we will convert this list of lists into a pandas DataFrame for easier manipulation and saving to CSV.
    rows = []
    for tr in soup.find_all("tr"):  # We loop through all the "tr" elements in the soup, which represent table rows. For each row, we find all the "td" elements, which represent the individual cells in that row. We check if there are at least 9 "td" elements, since we expect to extract 9 pieces of data for our columns. If there are enough columns, we extract the text from each of the first 9 "td" elements, strip any extra whitespace, and store it in a list called "row_data". We then append this list to our "rows" list, which will accumulate all the data from the summary table.
        cols = tr.find_all("td")  # For each table row (tr), we find all the table data cells (td) within that row. This gives us a list of columns for that row. We check if there are at least 9 columns, since we want to extract 9 specific pieces of information. If there are fewer than 9 columns, we skip this row and continue to the next one. If there are enough columns, we extract the text from each of the first 9 columns, strip any extra whitespace, and store it in a list called "row_data". We then append this list to our "rows" list, which will eventually contain all the data from the summary table.
        if len(cols) < 9:  # If there are fewer than 9 columns in this row, it means this row does not contain the data we are looking for (it might be a header row, a footer row, or just an incomplete row). In that case, we skip this row and move on to the next one by using "continue". This ensures that we only process rows that have the expected number of columns.
            # If there are at least 9 columns, we proceed to extract the text from each of the first 9 columns. We use a list comprehension to iterate over the first 9 columns (cols[:9]), get the text content of each column with get_text(strip=True) which also removes any extra whitespace, and store these values in a list called "row_data". This list will contain the data for one row of the summary table, corresponding to the columns we defined (Report Date, Start Time, End Time, etc.). We then append this "row_data" list to our "rows" list, which will accumulate all the rows of data from the summary table.
            continue
        # We create a list called "row_data" that contains the text from the first 9 columns of the current row. We use a list comprehension to iterate over the first 9 columns (cols[:9]), call get_text(strip=True) on each column to extract the text and remove any extra whitespace, and store these values in the "row_data" list. This list will represent one row of data from the summary table, with each element corresponding to a specific column (like Report Date, Start Time, etc.).
        row_data = [col.get_text(strip=True) for col in cols[:9]]
        rows.append(row_data)  # We append the "row_data" list to our "rows" list. This means that "rows" will be a list of lists, where each inner list contains the data for one row of the summary table. After we finish looping through all the "tr" elements, "rows" will contain all the data we extracted from the summary table, and we can then convert it into a pandas DataFrame for further processing and saving to CSV.

    columns = [  # We define a list of column names that correspond to the data we extracted from the summary table. These column names will be used when we create the pandas DataFrame, so that each column in the DataFrame has a meaningful name. The order of these column names should match the order of the data we extracted in "row_data" (Report Date, Start Time, End Time, etc.).
        "Report Date",
        "Start Time",
        "End Time",
        "Total Events",
        "Largest Flare",
        "C-class",
        "M-class",
        "X-class",
        "Proton Events",
    ]

# After we have collected all the rows of data in the "rows" list and defined our column names, we create a pandas DataFrame from this data. We pass the "rows" list as the data and the "columns" list as the column names to the pd.DataFrame constructor. This gives us a structured DataFrame where each row corresponds to a row from the summary table, and each column has a meaningful name. We then proceed to convert the "Report Date" column into datetime format, filter out any rows that are older than our specified MIN_YEAR, sort the DataFrame by "Report Date", and reset the index before returning it.
    df = pd.DataFrame(rows, columns=columns)
    df["Report Date"] = pd.to_datetime(
        df["Report Date"], format="%d-%b-%Y %H:%M", errors="coerce")
    df = df[df["Report Date"].dt.year >= MIN_YEAR]
    df = df.sort_values("Report Date").reset_index(drop=True)
    return df

# This function is called "update_summary_csv". It does not take any arguments and does not return anything (returns None). The purpose of this function is to update the summary CSV file with the latest data scraped from the archive summary table. It calls the "fetch_archive_summary_table" function to get the new data, loads the existing data from the CSV, combines them, removes duplicates, sorts by date, and then saves the updated DataFrame back to the CSV file.


def update_summary_csv() -> None:
    print("SUMMARY SCRAPER STARTED ...")
    new_df = fetch_archive_summary_table(BASE_URL)
    if new_df.empty:
        print("No summary rows scraped.")
        return

    print("Summary rows scraped this run:", len(new_df))

    # We load the existing summary CSV data into a DataFrame called "old_df". If the file does not exist or is empty, "old_df" will be an empty DataFrame. This allows us to combine it with the new data we just scraped without running into errors.
    old_df = load_existing(SUMMARY_CSV_PATH)
    combined = pd.concat(
        [old_df, new_df], ignore_index=True) if not old_df.empty else new_df

    combined["Report Date"] = pd.to_datetime(
        combined["Report Date"], errors="coerce")
    combined = combined.drop_duplicates(subset=["Report Date"], keep="last")
    combined = combined.sort_values("Report Date", ascending=False)

    combined.to_csv(SUMMARY_CSV_PATH, index=False)
    added = max(0, len(combined) - len(old_df))
    print(
        f"[OK] Wrote {len(combined)} rows to {SUMMARY_CSV_PATH}. Added {added} new rows.")

# This function is called "fetch_snapshot_links". It takes a base URL as input (a string) and returns a list of dictionaries. Each dictionary contains two keys: "snapshot_time" which is the text of the snapshot time link, and "snapshot_url" which is the full URL to that snapshot page. The function scrapes the base page for all snapshot time links, extracts their text and URLs, and collects them in a list until it encounters a snapshot that is older than the specified MIN_YEAR.


def fetch_snapshot_links(base_url: str) -> list[dict]:
    """Collect Snapshot Time links from base page (stops once older than MIN_YEAR)."""
    soup = get_soup(base_url)
    if soup is None:
        return []

    snapshots: list[dict] = []
# We loop through all the "tr" elements in the soup, which represent table rows. For each row, we find all the "td" elements, which represent the individual cells in that row. We check if there is at least one "td" element, and if so, we look for an "a" (anchor) tag within the first "td" that has an "href" attribute (indicating it's a link). If we find such a link, we extract the text of the link (which should be the snapshot time) and the URL it points to. We then parse the snapshot time text into a datetime object to check its year. If the year is older than our specified MIN_YEAR, we stop collecting snapshot links. Otherwise, we add the snapshot time and URL to our list of snapshots.
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 1:
            continue

        a = tds[0].find("a", href=True)
        if not a:
            continue

        snapshot_time_text = a.get_text(strip=True)
        snapshot_url = urljoin(base_url, a["href"])

        snap_dt = pd.to_datetime(
            snapshot_time_text, format="%d-%b-%Y %H:%M", errors="coerce")
        if pd.isna(snap_dt):
            continue

        if snap_dt.year < MIN_YEAR:
            break

        snapshots.append({"snapshot_time": snapshot_time_text,
                         "snapshot_url": snapshot_url})

    return snapshots


# This function is called "find_event_table". It takes a BeautifulSoup object as input and returns a BeautifulSoup object representing the event table if found, or None if not found. The function searches through all the tables in the soup and checks their text content for specific headers ("EName", "GOES Class", "Derived Position") that indicate it is the event table we want to scrape. If it finds such a table, it returns it; otherwise, it returns None.
def find_event_table(soup: BeautifulSoup):
    """Find the event table by checking for headers that include EName/GOES/Derived Position."""
    for table in soup.find_all("table"):
        header_text = table.get_text(" ", strip=True)
        if "EName" in header_text and "GOES Class" in header_text and "Derived Position" in header_text:
            return table
    return None

# This function is called "fetch_events_from_snapshot". It takes a snapshot time (as a string) and a snapshot URL (as a string) as input, and returns a pandas DataFrame containing the events scraped from that snapshot page. The function downloads the snapshot page, finds the event table, and extracts relevant data for each event (like EName, GOES Class, Derived Position, etc.) into a structured DataFrame format.


def fetch_events_from_snapshot(snapshot_time: str, snapshot_url: str) -> pd.DataFrame:
    """Scrape events from a single snapshot page."""
    soup = get_soup(snapshot_url)
    if soup is None:
        return pd.DataFrame()

    # We call the find_event_table function to search for the event table within the soup of the snapshot page. If we can't find the event table (i.e., if find_event_table returns None), we return an empty DataFrame, since there are no events to scrape from this page. If we do find the event table, we proceed to extract the relevant data from it.
    table = find_event_table(soup)
    if table is None:
        return pd.DataFrame()

    rows = []
    for tr in table.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 7:
            continue
# If there are at least 7 columns in the row, we extract the text from the first 7 columns, strip any extra whitespace, and store them in variables corresponding to the data we want (event number, EName, start time, stop time, peak time, GOES class, and derived position). We check if the EName is empty (which could indicate an invalid or header row), and if so, we skip that row. Otherwise, we append a list of the extracted data (including the snapshot time and URL) to our "rows" list, which will eventually be converted into a DataFrame.
        event_num = cols[0].get_text(strip=True)
        ename = cols[1].get_text(strip=True)
        start = cols[2].get_text(strip=True)
        stop = cols[3].get_text(strip=True)
        peak = cols[4].get_text(strip=True)
        goes_class = cols[5].get_text(strip=True)
        derived_pos = cols[6].get_text(strip=True)

        if not ename:
            continue

        rows.append([snapshot_time, snapshot_url, event_num, ename,
                    start, stop, peak, goes_class, derived_pos])

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=[
        "Snapshot Time",
        "Snapshot URL",
        "Event#",
        "EName",
        "Start",
        "Stop",
        "Peak",
        "GOES Class",
        "Derived Position",
    ])

 # This function is called "update_events_csv". It does not take any arguments and does not return anything (returns None). The purpose of this function is to update the events CSV file with the latest data scraped from all the snapshot pages. It collects snapshot links, scrapes events from each snapshot, combines them with existing data, removes duplicates, sorts by date, and saves the updated DataFrame to a CSV file.


def update_events_csv() -> None:
    print("EVENT SCRAPER STARTED ...")

    snapshots = fetch_snapshot_links(BASE_URL)
    if not snapshots:
        print("No snapshot links found.")
        return

    print("Snapshot links found:", len(snapshots))

    all_events: list[pd.DataFrame] = []
    skipped_pages = 0
    broken_urls: list[str] = []

# We loop through each snapshot link we collected. For each snapshot, we call the "fetch_events_from_snapshot" function to scrape the events from that snapshot page. If the resulting DataFrame is empty (which could happen if the page was not found or if there was no event table), we increment the "skipped_pages" counter and add the URL to the "broken_urls" list for logging. If we successfully scraped events, we append the DataFrame to our "all_events" list. We also print progress every 20 snapshots to keep track of how many we've processed.
    for i, s in enumerate(snapshots, start=1):
        df = fetch_events_from_snapshot(s["snapshot_time"], s["snapshot_url"])
        if df.empty:
            skipped_pages += 1
            broken_urls.append(s["snapshot_url"])
            continue

        all_events.append(df)

        if i % 20 == 0:
            print(f"Progress: {i}/{len(snapshots)} snapshots visited...")

    print("Snapshots skipped (404/no data/etc):", skipped_pages)

# If there were any broken URLs (snapshots we couldn't scrape), we print an example of one of those URLs. This can help with debugging and understanding if there are issues with certain snapshot pages.
    if broken_urls:
        print("Example skipped URL:", broken_urls[0])

    if not all_events:
        print("No event rows scraped. Nothing to write.")
        return

    new_df = pd.concat(all_events, ignore_index=True)
    print("Event rows scraped this run:", len(new_df))

    old_df = load_existing(EVENTS_CSV_PATH)
    combined = pd.concat(
        [old_df, new_df], ignore_index=True) if not old_df.empty else new_df

    combined["Snapshot Time Parsed"] = pd.to_datetime(
        combined["Snapshot Time"], format="%d-%b-%Y %H:%M", errors="coerce"
    )
    for col in ["Start", "Stop", "Peak"]:
        combined[col] = pd.to_datetime(combined[col], errors="coerce")

    combined = combined.sort_values(
        ["Snapshot Time Parsed", "Peak"], ascending=[False, False])

    combined = combined.drop_duplicates(subset=["EName"], keep="first")

    combined = combined.drop(columns=["Snapshot Time Parsed"])

    combined.to_csv(EVENTS_CSV_PATH, index=False)

    old_len = len(old_df) if not old_df.empty else 0
    added = max(0, len(combined) - old_len)
    print(
        f"[OK] Wrote {len(combined)} unique events to {EVENTS_CSV_PATH}. Added {added} new unique events.")


# This function is called "query_events_csv". It does not take any arguments and does not return anything (returns None). The purpose of this function is to read the events CSV file and print out some information about the events it contains. It checks if the CSV file exists, loads it into a DataFrame, prints the total number of unique events, and then filters for X-class events to display some details about them.
def query_events_csv() -> None:
    if not os.path.exists(EVENTS_CSV_PATH):
        print(f"No events CSV yet. Run: python {os.path.basename(__file__)}")
        return

    df = pd.read_csv(EVENTS_CSV_PATH, dtype=str)
    print("Total unique events in CSV:", len(df))
# If the "GOES Class" column exists in the DataFrame, we filter the DataFrame to find rows where the "GOES Class" starts with "X" (indicating X-class solar events). We then print out the details of these X-class events, showing the EName, Peak time, GOES Class, and Derived Position for up to 20 of these events. If no X-class events are found, we print a message indicating that none were found.
    if "GOES Class" in df.columns:
        x = df[df["GOES Class"].str.startswith("X", na=False)]
        print("\nX-class events (up to 20):")
        if x.empty:
            print("None found.")
        else:
            print(x[["EName", "Peak", "GOES Class", "Derived Position"]].head(
                20).to_string(index=False))


# The main block of the code checks if the script is being run directly (as the main program) rather than imported as a module. It then looks at the command line arguments to determine what action to take. If the first argument is "query", it calls the query_events_csv function to display information about the events in the CSV. If the first argument is "summary", it calls the update_summary_csv function to update the summary CSV file. If no arguments are provided, it defaults to calling the update_events_csv function to scrape and update the events CSV file.
if __name__ == "__main__":
    # python lmsal_archive_scrape.py           -> update events CSV (assignment)
    # python lmsal_archive_scrape.py query     -> query events CSV
    # python lmsal_archive_scrape.py summary   -> update summary CSV (optional)

    if len(sys.argv) > 1 and sys.argv[1].lower() == "query":
        query_events_csv()
    elif len(sys.argv) > 1 and sys.argv[1].lower() == "summary":
        update_summary_csv()
    else:
        update_events_csv()
