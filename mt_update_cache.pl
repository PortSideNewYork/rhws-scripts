#!/usr/bin/perl

#------------------------------------------------------------------------
# mt_update_cache.pl
# David Levine, PortSide NewYork, 2016
# Description:
# Sync data from MarineTraffic
#
# Runs every 2 minutes via cron
#------------------------------------------------------------------------

use strict;

use JSON;
use DB_File;
use Time::Piece;
use Locale::Country;

my $debug = 0;

my $gmtime = gmtime;
#my $localtime = localtime;

#Set working directory, since we do some file stuff here
chdir("/home/portside/mtupd");

#-----Get Marine Traffic API key from file-----
my $apikey;
my $api_file = "marine_traffic_api_key.txt";

open (my $fh, '<', $api_file) or die "Cannot open $api_file";
{
    local $/;
    $apikey = <$fh>;
}
close($fh);

chomp $apikey;



#----process extended data from marinetraffic; 
#    we can only update this once per hour
#    we'll cache this since things go in and out of our area 
#    in less than an hour

#----Read from/Save to hash file
tie (my %extended, 'DB_File', 'extended_info');

my $extended_url = "http://services.marinetraffic.com/api/exportvessels/${apikey}?protocol=json&msgtype=extended";

#----first check whether it's less than an hour since last update
my $timenow = time(); #seconds since 1/1/1970

my $lasttime = $extended{'LASTTIME'};

#----update if no previous run, or last run is more than 1 hour ago
if ( (! $lasttime) || ( ($timenow - $lasttime) > (60*60))) {
    
    #----Call web service to get data
    my $datastring = `wget -q -O - "$extended_url"`;

    #----Save received data to file for debugging, etc.
    open(my $temp, '>', "extended.txt");
    print $temp $datastring . "\n";
    close($temp);
    
    my $data = 0;

    eval {
	$data = decode_json($datastring);
	1;
    } or do {
	my $e = $@;
	#nothing - not valid JSON
    };
	

    for (my $ind = 0; $data && $ind < @{$data}; $ind++) {
	my $vessel = $data->[$ind];

	if ($debug) {
	    print $vessel->[0] . ": " . encode_json($vessel) . "\n";
	
	    if ($extended{$vessel->[0]}) {
		print $vessel->[0] . " exists in extended db\n";
	    }
	    else {
		print $vessel->[0] . " doesn't exist in extended db\n";
	    }
	}

	#----Map array to hash for sanity's sake
	my %vh;
	$vh{"MMSI"}   = $vessel->[0];
	$vh{"LAT"}    = $vessel->[1];
	$vh{"LONG"}   = $vessel->[2];
	$vh{"SPEED"}  = $vessel->[3];
	$vh{"COURSE"} = $vessel->[4];
	$vh{"TIMESTAMP"} = $vessel->[5] . "Z";
	$vh{"SHIPNAME"} = $vessel->[6];
	$vh{"SHIPTYPE"} = $vessel->[7];
	$vh{"IMO"}    = $vessel->[8];
	$vh{"CALLSIGN"} = $vessel->[9];

	#----Update FLAG to country name if not US or UK
	my $flag = $vessel->[10];
	if ($flag && $flag ne "US" && $flag ne "UK") {
	    $vh{"FLAG"} = code2country($flag);
	}
	else {
	    #----US or UK or blank
	    $vh{"FLAG"} = $flag;
	}

	$vh{"CURRENT_PORT"} = $vessel->[11];
	$vh{"LAST_PORT"} = $vessel->[12];
	$vh{"LAST_PORT_TIME"} = $vessel->[13] . "Z";
	$vh{"DESTINATION"} = $vessel->[14];
	$vh{"ETA"} = $vessel->[15] . "Z";
	$vh{"LENGTH"} = $vessel->[16];
	$vh{"WIDTH"} = $vessel->[17];
	$vh{"DRAUGHT"} = $vessel->[18];
	$vh{"GRT"} = $vessel->[19];
	$vh{"DWT"} = $vessel->[20];
	$vh{"YEAR_BUILT"} = $vessel->[21];
	
	$extended{$vessel->[0]} = encode_json(\%vh);
    }

    #----Save last time extended data was retrieved
    if ($data) {
	$extended{'LASTTIME'} = $timenow;
    }

}
else {
    print STDERR "Not long enough interval to update extended data\n" if ($debug);
}


#----Process simple------

#----Save simple info in hash file
tie (my %simple, 'DB_File', 'simple_info');

my $simple_url = "http://services.marinetraffic.com/api/exportvessels/${apikey}?protocol=json";

#----Update simple data if more than 2 minutes since last update
$timenow = time();
$lasttime = $simple{'LASTTIME'};

if ( (! $lasttime) || ( ( $timenow - $lasttime ) > (2 * 60) ) ) {

    my $datastring = `wget -q -O - "$simple_url"`;
    if ($datastring) {

	#----Save data received to file for debugging, etc.
	open(my $temp, '>', "simple.txt");
	print $temp $datastring . "\n";
	close($temp);

    	my $data = 0;

	eval {
	    $data = decode_json($datastring);
	    1;
	} or do {
	    my $e = $@;
	    #Not valid JSON - do nothing
	};


	#----Purge some old vessels----
	#$localtime is the current time
	#$gmtime is current time
	#loop through old simple data and wipe out anything where:
	# a) TIMESTAMP is older than 5 hours & speed was greater than 0.5 kts, or
	# b) speed was greater than 2 knts, or
	# c) speed <= 0.5 kts and older than 48 hrs

	if ($data) {
	    my $hours_5  = 5*60*60;
	    my $hours_48 = 48*60*60;
	
	    foreach my $simplekey (keys %simple) {
		next if ($simplekey eq "LASTTIME");
	    
		my $simpledata = decode_json($simple{$simplekey});

		#Add Z to end of timestamp if missing
		my $temp_ts = $simpledata->{'TIMESTAMP'};
		if ($temp_ts !~ /Z$/) {
		    $temp_ts .= "Z";
		}
	    
		my $vessel_ts = Time::Piece->strptime($temp_ts,
						  "%Y-%m-%dT%TZ");
		my $vessel_speed = $simpledata->{'SPEED'} / 10;

		my $diff = $gmtime - $vessel_ts;

		if ( (($diff >= $hours_5) && ($vessel_speed > 0.5))
		     || (($diff >= $hours_48) && ($vessel_speed <= 0.5))
		     || ($vessel_speed >= 2)
		    ) {
		    print STDERR "Deleting $simplekey  with TS " 
			.  $simpledata->{'TIMESTAMP'} 
		    . " and speed $vessel_speed kts\n" if ($debug);
		    
		    delete $simple{$simplekey};
		}
	    }
	}


	for (my $ind = 0; $data && $ind < @{$data}; $ind++) {
	    my $vessel = $data->[$ind];

	    if ($debug) {
		print $vessel->[0] . ": " . encode_json($vessel) . "\n";
		
		if ($simple{$vessel->[0]}) {
		    print $vessel->[0] . " exists in simple db\n";
		}
		else {
		    print $vessel->[0] . " doesn't exist in simple db\n";
		}
	    }

	    #-----map array to hash for sanity's sake
	    my %vh;
	    $vh{"MMSI"} = $vessel->[0];
	    $vh{"LAT"}  = $vessel->[1];
	    $vh{"LONG"}  = $vessel->[2];
	    $vh{"SPEED"}  = $vessel->[3];
	    $vh{"COURSE"}  = $vessel->[4];
	    $vh{"STATUS"}  = $vessel->[5];
	    $vh{"TIMESTAMP"}  = $vessel->[6] . "Z";

	    $simple{$vessel->[0]} = encode_json(\%vh);
	}

	#-----Save last time simple data was retrieved
	if ($data) {
	    $simple{'LASTTIME'} = $timenow;
	}
    }

}
else {
    print STDERR "Not long enough interval to update simple data\n" if ($debug);
}


#-----Now merge the two feeds to output, and put under WEB ROOT

my $vcount = 0;
my @outputdata;
    

#Loop through simple data, since those are the ones currently in range
foreach my $vesselkey (keys %simple) {

    next if ($vesselkey eq "LASTTIME");

    $vcount++;
	
    my $simpledata = decode_json($simple{$vesselkey});
    
    my $ext = $extended{$vesselkey};
    if ($ext) {
	my $extdata = decode_json($ext);

	#Merge simpledata into extdata, overwrite values in extdata
	#where duplicate keys
	#See http://learn.perl.org/faq/perlfaq4.html#How-do-I-merge-two-hashes
	@{$extdata}{keys %{$simpledata}} = values %{$simpledata};

	push(@outputdata, $extdata);
    }
    else {
	#just simple data; this is tricky, since we just have an MMSI and no name
	#Need to check for that on client
	push(@outputdata, $simpledata);
    }

}

#-----Dummy add in MARY A. WHALEN

my %mary;
$mary{"LAT"} = "40.680700";
$mary{"LONG"} = "-74.012783";
$mary{"SPEED"} = "0";
$mary{"COURSE"} = "47";
#$mary{"MMSI"} = "000000000";
$mary{"MMSI"} = "-5227445"; #what MarineTraffic uses
$mary{"SHIPNAME"} = "MARY A. WHALEN";
$mary{"FLAG"} = "US";
$mary{"LENGTH"} = "52.4"; #172 ft in meters
$mary{"YEAR_BUILT"} = "1938";
$mary{"STATUS"} = "5"; #moored
$mary{"SHIPTYPE"} = "80"; #tanker

$mary{"TIMESTAMP"} = $gmtime->datetime . "Z";

push(@outputdata, \%mary);
$vcount++;

print STDERR "Total $vcount vessels\n\n" if ($debug);

#----Create JSON output file for use by web app; in temp directory
#    so no current pull is messed up.
my $outputfile_name = "mtdata.json";
open (my $outputfile, '>', $outputfile_name) 
    || die "Cannot open $outputfile_name";

print $outputfile encode_json(\@outputdata) . "\n";
    
close($outputfile);


#----Now move file under web root
`mv --backup=simple $outputfile_name /var/www`;

die "Cannot move $outputfile_name to /var/www" if ($?);

exit(0);
