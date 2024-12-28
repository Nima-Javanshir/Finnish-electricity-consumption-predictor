import json
import requests
import pandas as pd
from io import StringIO
import urllib.request, json
import openpyxl

def Fingrid_dataset_name (dataset_id):
    try:
        url = f"https://data.fingrid.fi/api/datasets/{dataset_id}"

        hdr = {
        # Request headers
        'Cache-Control': 'no-cache',
        'x-api-key': '------------',
    }

        req = urllib.request.Request(url, headers=hdr)

        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
        result = json.loads(response.read().decode('utf-8'))
        result = pd.DataFrame([result]) 
        name=result['nameEn'].iloc[0];
        unit=result['unitEn'].iloc[0];
        return name,unit        
    except Exception as e:
        print(f"An error occurred: {e}")  


def Fingrid_data_fetch (start_time,end_time, dataset_id):
    try:
        url = f"https://data.fingrid.fi/api/datasets/{dataset_id}/data?startTime={start_time}&endTime={end_time}&format=csv&pageSize=20000&locale=en&sortBy=startTime&sortOrder=asc"
        hdr ={
        # Request headers
        'Cache-Control': 'no-cache',
        'x-api-key': '-------------',
        }
        req=urllib.request.Request(url, headers=hdr)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
        if response.getcode()==200:
            response_data=json.loads(response.read().decode('utf-8'))
            csv_data=response_data['data']
            data=pd.read_csv(StringIO(csv_data),sep=';')
            data.drop(columns='datasetId',inplace=True)
            data['startTime']=pd.to_datetime(data['startTime']).dt.tz_localize(None)
            data['endTime']=pd.to_datetime(data['endTime']).dt.tz_localize(None)
            data.set_index('startTime',inplace=True)
            data.drop(columns='endTime',inplace=True)
            data=data.resample('H').mean()
            data.reset_index(inplace=True)
            data.to_excel('Output2.xlsx',index=False)

            print (f"Data is sucessfullly written into file {'output.xlsx'}")
        else:
            print (f"Failed to fethc data. HTTP status code is [{response.getcode()}]")
    except Exception as e:
        print (f"an error occured: {e}")




start_time="2024-08-29"
end_time="2024-12-20"
dataset_id=124


Fingrid_data_fetch (start_time,end_time, dataset_id)

