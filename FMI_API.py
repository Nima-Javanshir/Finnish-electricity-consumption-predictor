#pip install fmiopendata
#pip install eccode
#pip install rasterio

import datetime as dt
from fmiopendata.wfs import download_stored_query
import datetime as dt
import pandas as pd
from fmiopendata.wfs import download_stored_query

def FMI_data_fetch(start_time, end_time, target_location, chunk_hours=168):
    # Convert strings to datetime objects
    start_time = dt.datetime.fromisoformat(start_time.replace("Z", ""))
    end_time = dt.datetime.fromisoformat(end_time.replace("Z", ""))

    data = []

    while start_time < end_time:
        # Calculate the chunk's end time
        chunk_end_time = min(start_time + dt.timedelta(hours=chunk_hours), end_time)

        # Format times for the API
        start_time_str = start_time.isoformat(timespec="seconds") + "Z"
        chunk_end_time_str = chunk_end_time.isoformat(timespec="seconds") + "Z"

        # Fetch the data
        obs = download_stored_query(
            "fmi::observations::weather::cities::multipointcoverage",
            args=[
                "bbox=61,59,21,26",  # Bounding box coordinates
                "starttime=" + start_time_str,
                "endtime=" + chunk_end_time_str,
                "timeseries=True",
            ]
        )

        # Process data for the target location
        for location, values in obs.data.items():
            if location == target_location:
                times = values["times"]
                air_temp_values = values.get("Air temperature", {}).get("values", [])
                wind_speed=values.get("Wind speed",{}).get("values", [])
                gust_speed=values.get("Gust speed",{}).get("values", [])

                if air_temp_values and wind_speed and gust_speed:  
                    for i, time in enumerate(times):
                        data.append({
                            "Timestamp": time,
                            "Air Temperature": air_temp_values[i],
                            "wind_speed": wind_speed[i],
                            "gust_speed": gust_speed[i]
                        })

        # Move to the next chunk
        start_time = chunk_end_time

    return data



# Define the overall start and end times
overall_start_time = "2018-01-25T00:00:00Z"
overall_end_time = "2018-06-01T00:00:00Z"
target_location = "Vantaa Helsinki-Vantaan lentoasema"

# Fetch data
data = FMI_data_fetch(overall_start_time, overall_end_time, target_location)

# Convert to a DataFrame
df = pd.DataFrame(data)
df.set_index('Timestamp',inplace=True)
df_hourly=df.resample('H').mean()
df_hourly.to_excel('Hourly consumption data from FMI.xlsx')
print (f"Data is successfully saved into Hourly temperature {target_location}.xlsx file")