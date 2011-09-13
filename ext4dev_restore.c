#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#include <linux/fs.h>
#include <linux/fiemap.h>

#include <stdint.h>

#include <ftw.h>
#include "e2p/e2p.h"

#define MNT "/mnt/temp"

#define SNAPSHOT_SHIFT 0
#define MAX 50
#define MAX_FILE_NAME 1024
#define SECTOR_SIZE 512

#define SNAP_MAGIC 0x70416e53 /* 'SnAp' */
#define SNAPSHOT_DISK_VERSION 1
#define SNAPSHOT_DELETED_FL 0x30

#define INITIAL_OFFSET SECTOR_SIZE
/*
 * List of snapshots sorted according to their IDs
 */

struct snapshot_list {
	char name[MAX_FILE_NAME];
	int id;
	struct snapshot_list *next;
	struct snapshot_list *prev;
} *list_head, *target_snapshot;

struct disk_exception {
    uint64_t old_chunk;
    uint64_t new_chunk;
};

/*
 * In-memory list that holds starting and ending logical
 * offsets in metadata chunks
 */
struct mdata_range {
	unsigned long blk_no;
	uint64_t start, end;
	struct mdata_range *next;
} *mdata_range_list_head;

char device_path[MAX]={0},
	command[MAX], 
	snapshot_file[MAX]={0}, 
	snapshot_dir[MAX], 
	mount_point[MAX],
	lvm_image_path[MAX];

int mdata_blocks=0, verbose;

/*
 * This function called from ftw adds snapshots one by one into the snapshot list.
 * Uses insertion sort algorithm to insert it into correct position
 */

int add_to_snapshot_list(const char *fpath, const struct stat *sb, int type)
{	
	struct snapshot_list *list_node;
	unsigned long id;

	if(type != FTW_F)
		return 0; 
	
	if(fgetversion(fpath, &id) < 0) {
		fprintf(stderr, "\nFailed to get snapshot id");
		return -1;
	}

	if(is_deleted(fpath)) {
		if(verbose) 
			fprintf(stderr, "\nDeleted snapshot : %s\n", fpath);
		return 0;
	}

	if(list_head == NULL) {
		list_node = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
		strcpy(list_node->name, fpath);
		list_node->id = id;
		list_head = list_node;
		list_head->next = NULL;
		list_head->prev = NULL;
	}
	else if (list_head->next == NULL) {
		list_node = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
		strcpy(list_node->name, fpath);
		list_node->id = id;

		if(list_head->id < id) {
			list_node->next = list_head;
			list_node->prev = NULL;

			list_head->prev = list_node;
			list_head = list_node;
		}
		else {
			list_head->next = list_node;

			list_node->prev = list_head;
			list_node->next = NULL;
		}
	}
	else {
		if(id > list_head->id) {
			list_node = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
			strcpy(list_node->name, fpath);
			list_node->id = id;

			list_node->next = list_head;
			list_node->prev = NULL;

			list_node->next->prev = list_node;
			list_head = list_node;
		}
		else {
			struct snapshot_list *temp;

			list_node = list_head;
			while(list_node->next && (list_node->next->id > id))
				list_node = list_node->next;

			temp = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
			strcpy(temp->name, fpath);
			temp->id = id;
			temp->next = list_node->next;
			if(temp->next)
				temp->next->prev = temp;
			temp->prev = list_node;

			list_node->next = temp;
		}
	}
	return 0;
}

/*
 * Sets target snapshot pointer in snapshot linked list
 */
int set_target_snapshot(char *snapshot_file)
{
	struct snapshot_list *t = list_head;

	while(t && strcmp(t->name, snapshot_file))
		t = t->next;

	if(t && !strcmp(t->name, snapshot_file)) {
		target_snapshot = t;
		return 1;
	}
	fprintf(stderr, "Snapshot %s not found (or deleted)\n", snapshot_file);
	umount_device(mount_point);
	rmdir(mount_point);
	exit(EXIT_FAILURE);
}

/*
 * Helper function for reading the snapshot list
 */
int read_list(void)
{
	struct snapshot_list *node = list_head, *end;

	do {
		fprintf(stderr, "\n%s\n", node->name,list_head->name);
		end = node;
		node = node->next;
	} while(node != NULL); 

	while(end) {
		fprintf(stderr, "\n%s\n", end->name);
		end = end->prev;
	}
	return 0;
}

/*
 * Input-
 * fd: Open file descriptor to read fiemap from
 *
 * The function applies fiemap ioctl on fd and reads fiemaps
 */
struct fiemap *read_fiemap(int fd)
{
        struct fiemap *fiemap;
        int extents_size;
	unsigned long seek;

        if ((fiemap = (struct fiemap*)malloc(sizeof(struct fiemap))) == NULL) {
                fprintf(stderr, "Out of memory allocating fiemap\n");   
                return NULL;
        }
        memset(fiemap, 0, sizeof(struct fiemap));

        fiemap->fm_start = 0;
        fiemap->fm_length = ~0;
        fiemap->fm_flags = 0;
        fiemap->fm_extent_count = 0;
        fiemap->fm_mapped_extents = 0;

        if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
                fprintf(stderr, "fiemap ioctl() failed\n");
                return NULL;
        }

        extents_size = sizeof(struct fiemap_extent) * 
                              (fiemap->fm_mapped_extents);

        if ((fiemap = (struct fiemap*)realloc(fiemap,sizeof(struct fiemap) + 
                                         extents_size)) == NULL) {
                fprintf(stderr, "Out of memory allocating fiemap\n");   
                return NULL;
        }

        memset(fiemap->fm_extents, 0, extents_size);
        fiemap->fm_extent_count = fiemap->fm_mapped_extents;
        fiemap->fm_mapped_extents = 0;

        if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
                fprintf(stderr, "fiemap ioctl() failed\n");
                return NULL;
        }

        return fiemap;
}

/*
 * Is deleted snapshot?
 */
int is_deleted(char *snapshot)
{
	unsigned long flags=0;	
	fgetsnapflags(snapshot, &flags);
	return (flags & SNAPSHOT_DELETED_FL);
}

/*
 * Is mdata chunk full?
 */
int is_full(int offset)
{
	if((offset) % (SECTOR_SIZE/8) == 2)
		return 1;
	return 0;
}

/*
 * Sync_to_lvm_image flushes in memory exception_mdata and corresponding data chunks
 * to the LVM image
 * Called when in memory mdata chunk is full
 */
int sync_to_lvm_image(uint64_t *exception_mdata, int snapshot_fd)
{
	int i, j;
	uint64_t offset;
	char buf[SECTOR_SIZE];
	int lvm_cow_store = open(lvm_image_path, O_RDWR);
	if(lvm_cow_store < 0) {
		fprintf(stderr, "\nLVM cow store absent");
		exit(EXIT_FAILURE);
	}

	if(snapshot_fd < 0) {
		fprintf(stderr, "\nSnapshot file not open");
		exit(EXIT_FAILURE);
	}
	lseek(lvm_cow_store, 
	      INITIAL_OFFSET + (mdata_blocks * SECTOR_SIZE) +
	      (SECTOR_SIZE * (SECTOR_SIZE >> 4)) * (mdata_blocks),
	      SEEK_SET);
	if(verbose)
		fprintf(stderr, "SYNCING at offset %lu ... ", lseek(lvm_cow_store, 0, SEEK_CUR));	
	write(lvm_cow_store, exception_mdata, SECTOR_SIZE);

	for(i=0; i < (SECTOR_SIZE>>4); i++) {
		offset = exception_mdata[2*i];
		lseek(snapshot_fd, offset * SECTOR_SIZE, SEEK_SET);
		read(snapshot_fd, buf, SECTOR_SIZE);
			
		write(lvm_cow_store, buf, SECTOR_SIZE);
	}
	close(lvm_cow_store);
	memset((void *)exception_mdata, 0, SECTOR_SIZE);
	if(verbose)
		fprintf(stderr, "Done..\n\n");
	
	return 1;
}

/*
 * Create LVM snapshot header
 */
int init_snapshot_image(void)
{
	int lvm_cow_store;
	uint32_t header[128]={0};

	if((lvm_cow_store = open(lvm_image_path, O_CREAT | O_RDWR | O_TRUNC)) < 0)
		return -1;

	header[0] = SNAP_MAGIC;
	header[1] = 1;
	header[2] = SNAPSHOT_DISK_VERSION;
	header[3] = SECTOR_SIZE/512;

	lseek(lvm_cow_store, 0, SEEK_SET);
	write(lvm_cow_store, (void *)header, 512);
	close(lvm_cow_store);
	return 1;
}

/*
 * Exports ext4 snapshot to LVM snapshot format
 */
int create_lvmexport(void)
{
	struct fiemap *fiemap;
	int fd, lvm_cow_store, new_offset=1, offset=0, counter=2;
	int i, j, k, location;
	char command[MAX], *snapshot_name;
	struct snapshot_list *p;
	uint64_t *exception_mdata;
	struct disk_exception de;
	struct mdata_range *p_range = NULL;

	p = target_snapshot;

	exception_mdata = (uint64_t *)malloc(SECTOR_SIZE);
	if(!exception_mdata) {
		fprintf(stderr, "\nOut of memory");
		exit(EXIT_FAILURE);
	}

	memset(exception_mdata, 0, SECTOR_SIZE);

	if(!init_snapshot_image()) {
		fprintf(stderr, "\nFailed to initialize snapshot image");
		exit(EXIT_FAILURE);
	}

	while(p) {
		if ((fd = open(p->name, O_RDONLY)) < 0) {
			fprintf(stderr, "Cannot open file %s\n", p->name);
			return 0;
		}
		
		snapshot_name = p->name + strlen(snapshot_dir) + 1;
		sprintf(command, "snapshot.ext4dev enable %s", snapshot_name);
		system(command);
		fprintf(stderr, "\nExporting Snapshot : %s ... ", snapshot_name);

		fiemap = read_fiemap(fd);

		for (i=0;i<fiemap->fm_mapped_extents;i++) {
			if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
			   fiemap->fm_extents[i].fe_physical) {

				for(j=0; j < (fiemap->fm_extents[i].fe_length) / SECTOR_SIZE; j++) {
					if(verbose)
						fprintf(stderr, "Searching for %lu ... ", (long)(fiemap->fm_extents[i].fe_logical
										     - SNAPSHOT_SHIFT)/SECTOR_SIZE + j);
					location = search_in_area(exception_mdata,
								  2*(offset+j),
								  (fiemap->fm_extents[i].fe_logical
								   - SNAPSHOT_SHIFT)/SECTOR_SIZE + j);
				
					if(location < 0) {
						/*
						 * The block is not present in lvm image
						 * Building metadata chunk
						 */
				
						if(verbose)
							fprintf(stderr, " - Not Found - ");
						de.old_chunk
							= fiemap->fm_extents[i].fe_logical/SECTOR_SIZE
							+ j - SNAPSHOT_SHIFT/SECTOR_SIZE;
						de.new_chunk = new_offset++ + INITIAL_OFFSET/SECTOR_SIZE;
						exception_mdata[2*offset] = de.old_chunk;
						exception_mdata[2*offset+1] = de.new_chunk;
						if(verbose)						
							fprintf(stderr, "Writing at %d,%d\n", 2*offset,2*offset+1);
						offset++;
						counter += 2;
						if(is_full(counter)) {
							if(verbose)							
								fprintf(stderr, "\nMdata Chunk Full ... \n");
							exception_mdata[2*offset+1] = new_offset + INITIAL_OFFSET/SECTOR_SIZE;
							offset = 0;
							new_offset++;

							if(!p_range) {
								p_range =
									(struct mdata_range *)malloc(sizeof(struct mdata_range));
								if(!p_range) {
									fprintf(stderr, "\nOut of memory");
									exit(EXIT_FAILURE);
								}
								p_range->blk_no = mdata_blocks;
								p_range->start = exception_mdata[0];
								p_range->end = exception_mdata[(SECTOR_SIZE>>3)-2];
								p_range->next = NULL;
								mdata_range_list_head = p_range;
							}
							else {
								p_range->next = (struct mdata_range *)malloc(sizeof(struct mdata_range));
								if(!p_range->next) {
									fprintf(stderr, "\nOut of memory");
									exit(EXIT_FAILURE);
								}
								p_range = p_range->next;
								p_range->start = exception_mdata[0];
								p_range->end = exception_mdata[(SECTOR_SIZE>>3)-2];
								p_range->blk_no = mdata_blocks;
								p_range->next = NULL;
							}
							sync_to_lvm_image(exception_mdata, fd);
							mdata_blocks++;
						}
					}
					else {
						/*
						 * Block found in lvm image
						 */
						if(verbose)
							fprintf(stderr, " - Found\n");
					}
				}
			}
		}
		p = p->prev;
		fprintf(stderr, "Done");
		if(!p) {
			if(verbose)			
				fprintf(stderr, "\nFinal Sync...");
			sync_to_lvm_image(exception_mdata, fd);
		}
		close(fd);
	}

	fprintf(stderr,
		"\nExport complete.\nNumber of mdata chunks : %d, Number of data chunks <= %d.\n",
		mdata_blocks,
		(SECTOR_SIZE>>4)*mdata_blocks);

	umount_device(mount_point);
	return 0;
}
	
/*
 * dumps fiemap to temporary disk image 
 * called only if we are in revert mode
 */
void dump_fiemap(struct fiemap *fiemap, int snapshot_file, int disk_image)
{
	int i,j,k;
	void *buf;
	static int full=0;
	static unsigned long offset=0;

	for (i=0;i<fiemap->fm_mapped_extents;i++) {
		if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
		   fiemap->fm_extents[i].fe_physical) {
			buf = (void *)malloc(fiemap->fm_extents[i].fe_length);
			
			lseek(snapshot_file, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			read(snapshot_file, buf, fiemap->fm_extents[i].fe_length);			

			lseek(disk_image, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			write(disk_image, buf, fiemap->fm_extents[i].fe_length);
			free(buf);
		}
	}
}

/*
 * search_in_area searches for key in area from start to end
 */
int search_in_area(uint64_t *current_area, int end, uint64_t key)
{
	int this=1, start=0, location, lvm_cow_store;
	static int is_superblock = 1;
	uint64_t area[SECTOR_SIZE>>3];
	struct mdata_range *p = mdata_range_list_head;
	lvm_cow_store = open(lvm_image_path, O_RDWR);
	
	if(is_superblock) {
		is_superblock = 0;
		return -1;
	}

	while(p) {
		if((key >= p->start) && (key <= p->end)) {
			lseek(lvm_cow_store,
			      INITIAL_OFFSET +
			      p->blk_no * (SECTOR_SIZE>>4) * SECTOR_SIZE +
			      p->blk_no * SECTOR_SIZE,
			      SEEK_SET
			      );
			read(lvm_cow_store, area, SECTOR_SIZE);
			end = (SECTOR_SIZE>>3)-1;
			this = 0;
			break;
		}
		p = p->next;
	}
	close(lvm_cow_store);

	if(this)
		memcpy(area, current_area, SECTOR_SIZE);

	while((start<=end) && (area[start] != key))
		start += 2;

	if(start <= end)
	return start;
	return -1;
}

/*
 * Restores the device to the earlier snapshot.
 * Function assumes that the required disk image is created at /tmp/disk_image.img
 * We are here if we are in revert mode
 */
void restore_fs(void)
{
	int i, fd, error;
	int device, disk_image;
	void *buf;
	struct snapshot_list *node;
	struct fiemap *fiemap;
	char *snapshot_name;

	disk_image = creat("/tmp/disk_image.img", 0777);
	
	if(list_head == NULL) {
		fprintf(stderr, "\nNo snapshot taken");
		umount_device(device_path);
		exit(EXIT_FAILURE);
	}

	node = list_head;
		
	/*
	 * Iterate over the list, capture all changed blocks from snapshots
	 * dump them to temporary disk image
	 */
		
	do {
		snapshot_name = node->name + strlen(snapshot_dir) + 1;

#warning "Implement snapshot enable using ioctls"

		if ((fd = open(node->name, O_RDONLY)) < 0) {
			fprintf(stderr, "Cannot open file %s\n", node->name);
			umount_device(device_path);
			exit(EXIT_FAILURE);
		}
		else {
			sprintf(command, "snapshot.ext4dev enable %s", snapshot_name);
			fprintf(stderr, "\n%s\n",command);
			system(command);
		}
			
		if ((fiemap = read_fiemap(fd)) != NULL)
			dump_fiemap(fiemap, fd, disk_image);
		else {
			fprintf(stderr, "\nfiemap ioctl failed");
			umount_device(device_path);
			exit(EXIT_FAILURE);
		}
		close(fd);
		node = node->next;
		sprintf(command,"%s/%s",snapshot_dir,snapshot_name);
		fprintf(stderr, "com : %s \t file : %s\n",command,snapshot_file);
	} while(strcmp(command, snapshot_file));
		
	close(disk_image);

	disk_image=open("/tmp/disk_image.img", O_RDONLY);
	device=open(device_path, O_RDWR, 0660);

	fiemap = read_fiemap(disk_image);

	for (i=0;i<fiemap->fm_mapped_extents;i++) {
		if(fiemap->fm_extents[i].fe_logical !=
		   fiemap->fm_extents[i].fe_physical){

			buf = (void *)malloc(fiemap->fm_extents[i].fe_length);

			lseek(disk_image, fiemap->fm_extents[i].fe_logical, SEEK_SET);
			read(disk_image, buf, fiemap->fm_extents[i].fe_length);

			lseek(device, fiemap->fm_extents[i].fe_logical, SEEK_SET);
			write(device, buf, fiemap->fm_extents[i].fe_length);

			free(buf);
		}
	}
	close(disk_image);
	close(device);
	umount_device(mount_point);
}

int umount_snapshot(const char *fpath, const struct stat *sb, int type)
{
	char *snapshot_name;
	char snapshot_mnt[MAX];
	struct stat st_mnt;
	
	if(type != FTW_F)
		return 0; 

	snapshot_name = (char *)(fpath + strlen(snapshot_dir) + 1);
	sprintf(snapshot_mnt, "%s@%s", mount_point, snapshot_name);
	if(stat(snapshot_mnt, &st_mnt) == 0) {
		umount(snapshot_mnt);
		rmdir(snapshot_mnt);
	}
	if(verbose)
		fprintf(stderr, "\numount : %s", snapshot_mnt);
	return 0;
}

void help()
{
    fputs("Usage: ext4dev_restore [options] -s <snapshot_device> -d <output_device>\n\n", stderr);
    fputs("Options:\n", stderr);
    fputs("-i <file>\text4 snapshot\n", stderr);
    fputs("-d <file>\tThe target device\n", stderr);
    fputs("-o <file>\tLVM image path\n", stderr);
    fputs("-r\t\tRevert mode. Reverts the device to specifed snapshot.\n", stderr);
    fputs("-l\t\tLVM export mode. Exports ext4 snapshot to LVM snapshot format.\n", stderr);
    fputs("-v\t\tBe verbose\n\n", stderr);
    fputs("If no mode is specified, the program assumes revert mode\n", stderr);
}

int umount_device(char *device)
{
	int error;

	ftw(snapshot_dir, umount_snapshot, 10);
	if(verbose)	
		fprintf(stderr, "\numount : %s\n", device);
	error = umount(mount_point);
	if(error) {
		printf("\nerror umount device : %d\n", error);
		return error;
	}
	       
	rmdir(MNT);
	return error;
}

int parse_cmd_line(int argc, char *argv[])
{
	int should_lvm_export = 0;
	char c;

	verbose = 0;

	while((c = getopt(argc, argv, "i:d:o:lrvh")) != -1) {
		switch(c) {
		case 'i': strcpy(snapshot_file, optarg); break;
		case 'd': strcpy(device_path, optarg); break;
		case 'o': strcpy(lvm_image_path, optarg); break;
		case 'l': should_lvm_export = 1; break;
		case 'r': should_lvm_export = 0; break;
		case 'v': verbose = 1; break;
		case 'h': help(); exit(EXIT_SUCCESS);
		default : help(); exit(EXIT_SUCCESS);
		}
	}
	if(device_path[0]==0 || snapshot_file[0]==0 || lvm_image_path[0]==0) {
		fprintf(stderr, "Device or snapshot or output not specified.\n");
		help();
		exit(EXIT_FAILURE);
	}
	rmdir(mount_point);
	if(mkdir(MNT,S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
		fprintf(stderr, "Failed to create mount point, are you root?\n");
                exit(EXIT_FAILURE);
	}
	strcpy(mount_point, MNT);
	if(mount(device_path, mount_point, "ext4dev", 0, NULL)) {
		fprintf(stderr, "Error while mounting, is device specified correctly?");
		rmdir(mount_point);
                exit(EXIT_FAILURE);
	}

#warning "Remove snapshot.ext4dev config command"
	sprintf(command, "snapshot.ext4dev config %s %s",device_path,mount_point);
	system(command);

	return should_lvm_export;	
}

int main(int argc, char **argv)
{
        int should_lvm_export=0;
	struct snapshot_list *node;
	struct fiemap *fiemap;
	char temp[MAX];

	list_head = NULL;
	mdata_range_list_head = NULL;

	should_lvm_export = parse_cmd_line(argc, argv);

	sprintf(snapshot_dir, "%s/.snapshots", mount_point);
	sprintf(temp, "%s/%s", snapshot_dir, snapshot_file);
	strcpy(snapshot_file, temp);

	/*
	 * Creating snapshot list
	 */
	if(ftw(snapshot_dir, add_to_snapshot_list, 10) != 0) {
		fprintf(stderr, "\nCannot walk throught fs");
		umount_device(device_path);
		exit(EXIT_FAILURE);
	}

	/*
	 * Setting up a pointer to target snapshot
	 */
	set_target_snapshot(snapshot_file);

	if(verbose)
		fprintf(stderr, "\nTarget : %s \n",target_snapshot->name);

	if(should_lvm_export) {
		create_lvmexport();
	}
	else {
		restore_fs();
		sprintf(command, "fsck.ext4dev -fxp %s", argv[1]);
		system(command);
	}

        exit(EXIT_SUCCESS);
}
