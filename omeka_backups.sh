#!/bin/bash

#Backup up Omeka installation and data

#Requires mysql authentication info to have been set using mysql_config_editor.
#See http://dev.mysql.com/doc/refman/5.7/en/mysql-config-editor.html

#This script is run nightly for main omeka instance.  On demand for dev instances.


set -x

cd ~portside


#Optional argument with instance, e.g. "dev"

instance=$1

#create backup directory
backup_base_dir=~/omeka${instance}_backup
back_dir=${backup_base_dir}/$(date +"%Y-%m-%d")

mkdir -p $back_dir || exit 1

#rm -rfv $back_dir/*

cd $back_dir

dbname=omeka${instance}

if [[ -z "$instance" ]]
then
    omekadir=/var/www
else
    omekadir=/var/www/${instance}
fi

echo "$(date) Backing up $omekadir and database $dbname to $PWD" >/dev/stderr


#Back up mysql database
mysqldump -h localhost \
	$dbname >omeka_db_backup.sql

gzip omeka_db_backup.sql


#Back up Omeka FILES
tar czf $back_dir/omeka_file_backup.tar.gz \
	-C $omekadir \
	files \
	plugins \
	themes \
	application/config \
	db.ini \
	.htaccess


#Back up other config files
cp -v -r \
   /etc/php/7.0/apache2/php.ini \
   /etc/apache2 \
   .

#Note: php5 config no longer used, since update to ubuntu 16.04 & php7 4/27/16
#   /etc/php5/apache2/php.ini


#-------------------------------------------------------------
#  Do some cleanup
#  Leave backup from first of each month,
#  plus last ~60 days of backups
#  ("head -n -60" means all but last 60 lines)
#-------------------------------------------------------------
cd $backup_base_dir

ls  | grep -v '01$' | head -n -60 | xargs rm -rfv 
