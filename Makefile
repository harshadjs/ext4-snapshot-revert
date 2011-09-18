all:
	gcc ext4dev_restore.c -le2p -o ext4dev_restore

install:
	install -m +x ./ext4dev_restore /sbin/
	install -m +x ./lvm_revert.sh /sbin/
clean:
	rm /sbin/ext4dev_restore
	rm /sbin/lvm_revert.sh
	rm  ext4dev_restore
