import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import json

def get_stop_data():

    # Specify the URL containing the dataset and pass it to
    # urlopen() to get the HTML of the page.
    url = "http://www.psudataeng.com:8000/getStopEvents/"
    html = urlopen(url)

    # Create a BeautifulSoup object from the HTML!
    # BeautifulSoup package parses the HTML (takes the raw
    # html text and breaks it into Python objects).
    # 'lxml' is the HTML parser.
    soup = BeautifulSoup(html, 'html.parser')

    # To get table rows only, pass 'tr'  in soup.find_all()
    rows = soup.find_all('tr')

    # This loop iterates through table rows and grabs 
    # the cells of the rows. 'td' is the tag used to
    # delimit table cells.

    for row in rows:
        row_td = row.find_all('td')

    # Each row has html tags embedded. Remove the tags with BeautifulSoup (or regex).
    # Using BeauSoup, pass the string of interest into BeautifulSoup() and use get_text() 
    # to extract text without html tags.

    str_cells = str(row_td)
    cleantext = BeautifulSoup(str_cells, 'html.parser').get_text()

    # Same thing, but with regular expressions! 

    # Build a regex that finds all the characters inside 
    # the < td > html tags and replace them with an empty
    # string for each table row.

    list_rows = []
    for row in rows:
        cells = row.find_all('td')
        str_cells = str(cells)
        clean = re.compile('<.*?>')
        clean2 = (re.sub(clean, '', str_cells))
        list_rows.append(clean2)

    # Next, convert the list into a Pandas dataframe.

    df = pd.DataFrame(list_rows)

    # Data transformation! Split the '0' column into multiple columns at the comma position

    df1 = df[0].str.split(',', expand=True)

    # Still have unwanted square brackets surrounding each row.

    df1[0] = df1[0].str.strip('[')

    # Missing table headers! Get 'em.

    col_labels = 'vehicle_number,leave_time,train,route_number,direction,service_key,stop_time,arrive_time,dwell,location_id,door,lift,ons,offs,estimated_load,maximum_speed,train_mileage,pattern_distance,location_distance,x_coordinate,y_coordinate, data_source,schedule_status'
    all_header = []
    all_header.append(col_labels)

    df2 = pd.DataFrame(all_header)

    # Again, split column "0" into multiple columns at the comma position for all rows.
    df3 = df2[0].str.split(',', expand=True)

    # Next, concatenate the two dataframes into one using concat().

    frames = [df3, df1]
    df4 = pd.concat(frames)

    # Reconfigure the dataframe so that the first row is the table header.

    df5 = df4.rename(columns=df4.iloc[0])

    # Need to validate the data! Let's drop all rows with any missing values.

    df6 = df5.dropna(axis=0, how='any')

    # Header is replicated as the first row in df5 and df6. Drop this extra row!

    df7 = df6.drop(df6.index[0])

    # Remove the closing bracket for cells in the schedule_status col.
    df7['schedule_status'] = df7['schedule_status'].str.strip(']')
    #print(df7.head(20))
    
    result = df7.to_json(orient = 'records')
    parsed = json.loads(result)

    return parsed
