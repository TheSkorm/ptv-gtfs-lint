#designed for python 2.7

# pip install patool pyunpack

import requests
from pyunpack import Archive
import zipfile

SOURCE="http://data.ptv.vic.gov.au/downloads/gtfs.zip"
PATH="gtfs.zip"
UPDATE_DATA=False
# 4:"MetroBus",8:"NightRider"
TRUSTED_DATA={2:"MetroTrain",3:"MetroTram"}

if UPDATE_DATA:
    r = requests.get(SOURCE, stream=True)
    if r.status_code == 200:
        with open(PATH, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
Archive(PATH).extractall('temp/')

def merge_data(header,file_name):
    data = []
    for type in TRUSTED_DATA:
        Archive('temp/' + str(type) + "/google_transit.zip").extractall('extracted/'+TRUSTED_DATA[type])
        with open('extracted/'+TRUSTED_DATA[type]+'/'+file_name) as f:
            data += f.read().decode("utf-8-sig").splitlines()
    data=sorted(set(data))
    #remove the header and place at top again
    data.remove(header)
    data.insert(0,header)

    f = open('merged/'+file_name,'wb')
    for line in data:
        f.write("%s\n" % line)
    f.close()

merge_data("service_id,date,exception_type", "calendar_dates.txt")
merge_data("service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date", "calendar.txt")
merge_data("route_id,agency_id,route_short_name,route_long_name,route_type","routes.txt")
merge_data("shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled","shapes.txt")
merge_data("trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled","stop_times.txt")
merge_data("stop_id,stop_name,stop_lat,stop_lon","stops.txt")
merge_data("route_id,service_id,trip_id,shape_id,trip_headsign,direction_id","trips.txt")

zf = zipfile.ZipFile('output/gtfs.zip', mode='w')
zf.write('merged/agency.txt','agency.txt')
zf.write('merged/calendar.txt','calendar.txt')
zf.write('merged/calendar_dates.txt','calendar_dates.txt')
zf.write('merged/routes.txt','routes.txt')
zf.write('merged/shapes.txt','shapes.txt')
zf.write('merged/stop_times.txt','stop_times.txt')
zf.write('merged/stops.txt','stops.txt')
zf.write('merged/trips.txt','trips.txt')
zf.close()
