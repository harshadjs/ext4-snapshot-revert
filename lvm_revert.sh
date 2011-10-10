#!/bin/bash

VG="$1"
LV="$2"
SNAP="$3"
SHOULD_MERGE="$4"
LVM_SNAP="lvm_restore_snap_$SNAP"
EXT4DEV_RESTORE="/sbin/ext4dev_restore"
TEST_FILE="/tmp/test_file"

# check for correct command line params
if [ -z $VG ] | [ -z $LV ] | [ -z $SNAP ] | [ -z $SHOULD_MERGE ] ; then
    echo "Usage : lvm_revert.sh <VG> <LV> <Snapshot_name> <Should_merge ?>"
    echo ""
    echo "<VG> : Volume group containing target LV"
    echo "<LV> : Target logical volume"
    echo "<Snapshot> : Ext4dev snapshot name (and not the path)"
    echo "<Should_merge ?> : Boolean if set(1), merges LVM export into the LV, else does not merge."
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

$EXT4DEV_RESTORE -i $SNAP -o /dev/mapper/$VG-$LVM_SNAP-cow -d /dev/mapper/$VG-$LV -l
if [ $? -ne 0 ] ; then
    echo "ext4dev restore failed"
    lvremove -f $VG/$LVM_SNAP
    exit 1;
fi
cat /etc/mtab | grep /dev/mapper/$VG-$LV > /dev/null
if [ $? -ne 0 ] ; then
    vgchange -a n $VG
    vgchange -a y $VG
    fsck.ext4dev -fxp /dev/$VG/$LVM_SNAP
    if [ $SHOULD_MERGE == "1" ] ; then
	lvconvert --merge $VG/$LVM_SNAP > /dev/null
	if [ $? -ne 0 ] ; then
	    echo "Merging failed"
	    exit 1;
	fi
    fi
else
    fsck.ext4dev -fxp /dev/$VG/$LVM_SNAP
fi

