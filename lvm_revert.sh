#!/bin/bash

VG="$1"
LV="$2"
SNAP="$3"
LVM_SNAP="lvm_restore_snap_$SNAP"
EXT4DEV_RESTORE="./ext4dev_restore"
TEST_FILE="/tmp/test_file"

# check for correct command line params
if [ -z $VG ] | [ -z $LV ] | [ -z $SNAP ] ; then
    echo "Insufficient arguments"
    exit 1;
fi

# check for ext4dev_restore binary
if ! [ -x $EXT4DEV_RESTORE ] ; then 
    echo "ext4dev_restore binary not found"
    exit 1;
fi

# check for valid VG
vgs | grep $VG > /dev/null
if [ $? -ne 0 ] ; then
    echo "Volume Group $VG not found"
    exit 1;
else
    echo "VG = $VG"
fi

# check if LV is present in VG
(lvs | grep $LV) | while read a b c d e f g h i j; do
    if [ "$LV" == "$a" ] && [ "$VG" == "$b" ] ; then
	touch $TEST_FILE
    fi
done 

if ! [ -f $TEST_FILE ] ; then
    echo "$LV not found in $VG"
    exit 1;
else
    rm $TEST_FILE 2>/dev/null
fi

lvcreate -s -n $LVM_SNAP -L 1G $VG/$LV
echo $?
echo "#Warning : Fix size in lvcreate"
sleep 1

$EXT4DEV_RESTORE -i $SNAP -o /dev/mapper/$VG-$LVM_SNAP-cow -d /dev/$VG/$LV -l
if [ $? -ne 0 ] ; then
    echo "ext4dev restore failed"
    lvremove -f $VG/$LVM_SNAP
    exit 1;
fi
vgchange -a n $VG
vgchange -a y $VG
lvconvert --merge $VG/$LVM_SNAP

if [ $? -ne 0 ] ; then
    echo "Merging Failed"
    lvremove -f $VG/$LVM_SNAP
    exit 1;
fi

fsck.ext4dev -fxp /dev/$VG/$LV
