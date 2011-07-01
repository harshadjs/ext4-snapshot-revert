all:
	gcc ext4dev_restore.c -le2p -o ext4dev_restore

clean:
	rm ext4dev_restore
