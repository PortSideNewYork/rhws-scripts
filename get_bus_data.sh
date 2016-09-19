#!/bin/bash

#Gets list of MTA bus stations for B57 and B61 routes; doesn't include any BusTime
#status info
#So don't need to update very frequently

mta_developer_key="33b65555-37cd-4d1c-8cfd-70f3d6b9d2ea"

cd /home/portside/mtabus

for route in 57 61
do

    output_file=mta_nyct_b${route}.json
    
    wget -q --backups \
	 -O $output_file \
	 "http://bustime.mta.info/api/where/stops-for-route/MTA%20NYCT_B${route}.json?key=${mta_developer_key}&includePolylines=false&version=2"

    mv -b $output_file /var/www
    
done
