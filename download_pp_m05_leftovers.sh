#!/bin/bash

mdss_infl=$1
disk_infl=$2

download_files=($( cat $mdss_infl | grep -v 'grad_r_number' | grep -v 'turb_ke' ))
target_files=($( cat $disk_infl | grep -v 'grad_r_number' | grep -v 'turb_ke' ))

echo "Found ${#download_files[@]} files to download"
if [[ ${#download_files[@]} -ne ${#target_files[@]} ]]; then
     echo "Error, diff number between target and download files"
     exit 1
fi

MDSSFILE=mdss.tmp.file.txt
DISKFILE=disk.tmp.file.txt

rm $MDSSFILE || true
rm $DISKFILE || true

for fl in ${download_files[@]}; do 
    echo $fl >> $MDSSFILE
done

for fl in ${target_files[@]}; do
    fl2=$(echo $fl | sed -e 's/.sub.nc/.nc/')
    fl3=$(echo $fl2 | sed -e 's/\*//')
    echo $fl3 >> $DISKFILE
done

module load parallel

parallel --link -- mdss -P du7 get {1} {2} :::: $MDSSFILE $DISKFILE
