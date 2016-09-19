#!/usr/bin/perl

#-------------------------------------------------------------------------
# update_cb_cache.pl
#Retrieve data from Citi Bike web service, filter to just show
#desired area, and cache for use by Red Hook WaterStories
#Copyright 2016, PortSide New York
#-------------------------------------------------------------------------

use strict;

use JSON;

use DB_File;

my $debug = 0;

#Get file, decode JSON and put into perl data structure
sub get_json($) {
    my $jsfile = shift(@_);
    my $json;
    {
	local $/; #Enable 'slurp' mode
	open my $fh, "<", "$jsfile" || die "Can't open $jsfile";
	$json = <$fh>;
	close $fh;
    }
    
    my $data = decode_json($json);
    return $data;
}

#Set working directory, since we do some file stuff here
chdir("/home/portside/citibike");


#wget -q = no output

#Get station informantion
my $si_json = `wget --backups -q https://gbfs.citibikenyc.com/gbfs/en/station_information.json`;
die "Error retrieving station_information.json" if ($?);

my $si = get_json("station_information.json");

#Get station status
my $ss_json = `wget --backups -q https://gbfs.citibikenyc.com/gbfs/en/station_status.json`;
die "Error retrieving station_status.json" if ($?);

my $ss = get_json("station_status.json");


#Sample station info:
#{"last_updated":1472586838,"ttl":10,"data":{"stations":[{"station_id":"72","name":"W 52 St & 11 Ave","short_name":"6926.01","lat":40.76727216,"lon":-73.99392888,"region_id":71,"rental_methods":["KEY","CREDITCARD"],"capacity":39,"eightd_has_key_dispenser":false},{"station_id":"79","name":"Franklin St & W Broadway","short_name":"5430.08","lat":40.71911552,"lon":-74.00666661,"region_id":71,"rental_methods":["KEY","CREDITCARD"],"capacity":33,"eightd_has_key_dispenser":false},...

#sample station status
#{"last_updated":1472589045,"ttl":10,"data":{"stations":[{"station_id":"72","num_bikes_available":1,"num_bikes_disabled":0,"num_docks_available":38,"num_docks_disabled":0,"is_installed":1,"is_renting":1,"is_returning":1,"last_reported":"1472588622","eightd_has_available_keys":false},{"station_id":"79","num_bikes_available":26,"num_bikes_disabled":1,"num_docks_available":6,"num_docks_disabled":0,"is_installed":1,"is_renting":1,"is_returning":1,"last_reported":"1472588946","eightd_has_available_keys":false},{"last_updated":1472589045,"ttl":10,"data":{"stations":[{"station_id":"72","num_bikes_available":1,"num_bikes_disabled":0,"num_docks_available":38,"num_docks_disabled":0,"is_installed":1,"is_renting":1,"is_returning":1,"last_reported":"1472588622","eightd_has_available_keys":false},{"station_id":"79","num_bikes_available":26,"num_bikes_disabled":1,"num_docks_available":6,"num_docks_disabled":0,"is_installed":1,"is_renting":1,"is_returning":1,"last_reported":"1472588946","eightd_has_available_keys":false},...

my $stations = $si->{'data'}->{'stations'};
my $stations_status = $ss->{'data'}->{'stations'};

#print "Number of stations: " . @$stations . "\n";

#region 71 is NYC (70 is Jersey City)

#Filter by geography
my $minlong = -74.020379;
#my $maxlong = -73.978923;
my $maxlong = -73.971058;
#my $minlat  = 40.661489;
my $minlat  = 40.660115;
my $maxlat  = 40.704512;

my $count = 0;

#I think the array of station info and station status uses the same array indexing

my @results;


for (my $stind = 0; $stind < @$stations; $stind++) {
    my $station = $stations->[$stind];

    next if ($station->{'region_id'} != 71);


    next if ($station->{'lat'} < $minlat);
    next if ($station->{'lat'} > $maxlat);
    next if ($station->{'lon'} < $minlong);
    next if ($station->{'lon'} > $maxlong);

    my $station_status = $stations_status->[$stind];

    #print $station->{'name'} . "\n";
    if ($debug) {
	print "Station info:\n";
	while ( my ($key, $value) = each %$station) {
	    print "\t$key = $value\n";
	}

	print "Station status:\n";
	while ( my ($key, $value) = each %$station_status) {
	    print "\t$key = $value\n";
	}

	print "\n";
    }

    #Merge status into info
    while ( my ($key, $value) = each %$station_status) {
	$station->{$key} = $value;
    }

    push @results, $station;
    
    
    $count++;
}

print "Showing $count stations\n" if ($debug);

#Write out file that is used by browser
my $outfilename = "citibike_stations.json";

open(my $outfile, '>', $outfilename) || die "Cannot open $outfilename";

print $outfile encode_json(\@results) . "\n";

close($outfile);


#Now move file to under web root
`mv -b $outfilename /var/www`;

die "Cannot move $outfilename to /var/www" if ($?);

