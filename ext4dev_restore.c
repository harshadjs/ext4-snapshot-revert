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
#define LVM_IMAGE_PATH "./lvm_image.img"

#define SNAPSHOT_SHIFT 0
#define MAX 50
#define MAX_FILE_NAME 1024
#define SECTOR_SIZE 512

#define SNAP_MAGIC 0x70416e53 /* 'SnAp' */
#define SNAPSHOT_DISK_VERSION 1

#define INITIAL_OFFSET 512

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

struct mdata_range {
	unsigned long blk_no;
	uint64_t start, end;
	struct mdata_range *next;
} *mdata_range_list_head;

char device_path[MAX],
	command[MAX], 
	snapshot_file[MAX], 
	snapshot_dir[MAX], 
	mount_point[MAX];

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
		printf("\nFailed to get snapshot id");
		return -1;
	}

	printf("\n%s %ld", fpath, id);
	
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
 *
 */
int set_target_snapshot(char *snapshot_file)
{
	struct snapshot_list *t = list_head;

	while(t && strcmp(t->name, snapshot_file))
		t = t->next;

	if(!strcmp(t->name, snapshot_file)) {
		target_snapshot = t;
		return 1;
	}
		
	return 0;
}

/*
 * Helper function for reading the snapshot list
 */
int read_list(void)
{
	struct snapshot_list *node = list_head, *end;

	do {
		printf("\n%s\n", node->name,list_head->name);
		end = node;
		node = node->next;
	} while(node != NULL); 

	while(end) {
		printf("\n%s\n", end->name);
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

int is_full(int offset)
{
	if((offset) % (SECTOR_SIZE/8) == 2)
		return 1;
	return 0;
}

int sync_to_lvm_image(uint64_t *exception_mdata, int snapshot_fd)
{
	int i, j;
	uint64_t offset;
	char buf[SECTOR_SIZE];
	int lvm_cow_store = open(LVM_IMAGE_PATH, O_RDWR|O_CREAT);
	if(lvm_cow_store < 0) {
		printf("\nLVM cow store absent");
		exit(EXIT_FAILURE);
	}

	if(snapshot_fd < 0) {
		printf("\nSnapshot file not open");
		exit(EXIT_FAILURE);
	}
	lseek(lvm_cow_store, 0, SEEK_END);
	printf("SYNCING at offset %ld ... ", lseek(lvm_cow_store, 0, SEEK_CUR));	
	write(lvm_cow_store, exception_mdata, SECTOR_SIZE);

	for(i=0; i < (SECTOR_SIZE>>4); i++) {
		offset = exception_mdata[2*i];
		lseek(snapshot_fd, offset * SECTOR_SIZE, SEEK_SET);
		read(snapshot_fd, buf, SECTOR_SIZE);
			
		lseek(lvm_cow_store, 0, SEEK_END);
		write(lvm_cow_store, buf, SECTOR_SIZE);
	}
	close(lvm_cow_store);
	memset((void *)exception_mdata, 0, SECTOR_SIZE);
	printf("Done..\n\n");
	
	return 1;
}

int init_snapshot_image(void)
{
	int lvm_cow_store;
	uint32_t header[128];

	if((lvm_cow_store = open(LVM_IMAGE_PATH, O_CREAT | O_RDWR | O_TRUNC)) < 0)
		return -1;

	header[0] = SNAP_MAGIC;
	header[1] = 1;
	header[2] = SNAPSHOT_DISK_VERSION;
	header[3] = SECTOR_SIZE/512;
 
	write(lvm_cow_store, (void *)header, 512);
	close(lvm_cow_store);
	return 1;
}

int create_lvmexport(void)
{
	struct fiemap *fiemap;
	int fd, lvm_cow_store, new_offset=1, offset=0, counter=2, mdata_blocks=0;
	int i, j, k, location;
	char command[MAX], *snapshot_name;
	struct snapshot_list *p;
	uint64_t *exception_mdata;
	struct disk_exception de;
	struct mdata_range *p_range = NULL;

	p = target_snapshot;

	exception_mdata = (uint64_t *)malloc(SECTOR_SIZE);
	if(!exception_mdata) {
		printf("\nOut of memory");
		exit(EXIT_FAILURE);
	}

	memset(exception_mdata, 0, SECTOR_SIZE);

	if(!init_snapshot_image()) {
		printf("\nFailed to initialize snapshot image");
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
		printf("\n* * * * * * * * * * * * * * * * * * * Snapshot : %s * * * * * * * * * * * * * * * * * * * *\n\n", snapshot_name);

		fiemap = read_fiemap(fd);

		for (i=0;i<fiemap->fm_mapped_extents;i++) {
			if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
			   fiemap->fm_extents[i].fe_physical) {

				for(j=0; j < (fiemap->fm_extents[i].fe_length) / SECTOR_SIZE; j++) {
					printf("Searching for %ld ... ", (long)(fiemap->fm_extents[i].fe_logical
										     - SNAPSHOT_SHIFT)/SECTOR_SIZE + j);
					location = search_in_area(exception_mdata,
								  2*(offset+j),
								  (fiemap->fm_extents[i].fe_logical
								   - SNAPSHOT_SHIFT)/SECTOR_SIZE + j);
				
					if(location < 0) {
						/*
						 * The is not present in export list
						 * Writing metadata to metadata chunk
						 */
				
						printf(" - Not Found - ");
						de.old_chunk
							= fiemap->fm_extents[i].fe_logical/SECTOR_SIZE
							+ j - SNAPSHOT_SHIFT/SECTOR_SIZE;
						de.new_chunk = new_offset++ + INITIAL_OFFSET/SECTOR_SIZE;
						exception_mdata[2*offset] = de.old_chunk;
						exception_mdata[2*offset+1] = de.new_chunk;
						printf("Writing at %d,%d\n", 2*offset,2*offset+1);
						offset++;
						counter += 2;
						if(is_full(counter)) {
							printf("\nMdata Chunk Full ... ");
							exception_mdata[2*offset+k+1] = new_offset + INITIAL_OFFSET/SECTOR_SIZE;
							offset = 0;
							new_offset++;

							if(!p_range) {
								p_range =
									(struct mdata_range *)malloc(sizeof(struct mdata_range));
								if(!p_range) {
									printf("\nOut of memory");
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
									printf("\nOut of memory");
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
						printf(" - Found\n");
					}
					/*
					 * Block already present
					 * Just overwrite the corresponding data chunk
					 */
				}
			}
		}
		p = p->prev;
		if(!p) {
			printf("\nDone!!...Final Sync...");
			sync_to_lvm_image(exception_mdata, fd);
		}
		close(fd);
	}

	lvm_cow_store = open(LVM_IMAGE_PATH, O_RDONLY);
	printf("Number of metadata blocks : %d", mdata_blocks);
	close(lvm_cow_store);

	p_range = mdata_range_list_head;
	printf("\nException mdata\nid\tstart\tend\n");
	while(p_range) {
		printf("%ld\t%ld\t%ld\n",
		       p_range->blk_no, 
		       p_range->start,
		       p_range->end);
		p_range = p_range->next;
	}
	umount_device(mount_point);
	return 0;
}
	
/*
 * The funtion either dumps fiemap to temporary disk image or dumps it to
 * lvm export list depending upon the value of should_lvm_export
 * if set builds lvm export list
 * if reset builds temporary disk image
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
	uint64_t area[SECTOR_SIZE>>3];
	struct mdata_range *p = mdata_range_list_head;
	lvm_cow_store = open(LVM_IMAGE_PATH, O_RDWR);
	
	//	printf("\nTraversing mdata range list (key = %ld): |", key);
	while(p) {
		//	printf("%ld %ld|",p->start, p->end);
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
		printf("\nNo snapshot taken");
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
			printf("\n%s\n",command);
			system(command);
		}
			
		if ((fiemap = read_fiemap(fd)) != NULL)
			dump_fiemap(fiemap, fd, disk_image);
		else {
			printf("\nfiemap ioctl failed");
			umount_device(device_path);
			exit(EXIT_FAILURE);
		}
		close(fd);
		node = node->next;
		sprintf(command,"%s/%s",snapshot_dir,snapshot_name);
		printf("com : %s \t file : %s\n",command,snapshot_file);
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

	snapshot_name = (char *)(fpath + strlen(snapshot_dir) + 1);
	sprintf(snapshot_mnt, "%s@%s", mount_point, snapshot_name);
	if(stat(snapshot_mnt, &st_mnt) == 0) {
		umount(snapshot_mnt);
		rmdir(snapshot_mnt);
	}
	printf("\numount : %s", snapshot_mnt);
	return 0;
}


int umount_device(char *device)
{
	int error;

	if((error = ftw(snapshot_dir, umount_snapshot, 10)) != 0) {
		printf("\nCannot walk throught fs pop");
		return error;
	}

	error = umount(device);
	if(error)
		return error;
	rmdir(MNT);
	return error;
}

int parse_cmd_line(int argc, char *argv[])
{
	int should_lvm_export = 0;

        if (argc < 4) {
		printf("\next4dev_restore <device> <snapshot> [lvmexport/revert]\n");
                exit(EXIT_FAILURE);
        }

	if(argc == 4 && !strcmp(argv[3],"lvmexport"))
		should_lvm_export = 1;

	strcpy(device_path, argv[1]);
	if(mkdir(MNT,S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
		printf("\nFailed to create mount point");
                exit(EXIT_FAILURE);
	}
	strcpy(mount_point, MNT);

	if(mount(argv[1], mount_point, "ext4dev", 0, NULL)) {
		printf("\nError while mounting");
                exit(EXIT_FAILURE);
	}

#warning "Remove snapshot.ext4dev config command"
	sprintf(command, "snapshot.ext4dev config %s %s",argv[1],mount_point);
	system(command);

	return should_lvm_export;	
}

int main(int argc, char **argv)
{
        int should_lvm_export=0;
	struct snapshot_list *node;
	struct fiemap *fiemap;

	list_head = NULL;
	mdata_range_list_head = NULL;

	should_lvm_export = parse_cmd_line(argc, argv);

	sprintf(snapshot_dir, "%s/.snapshots", mount_point);
	sprintf(snapshot_file, "%s/%s", snapshot_dir, argv[2]);

	/*
	 * Creating snapshot list
	 */
	if(ftw(snapshot_dir, add_to_snapshot_list, 10) != 0) {
		printf("\nCannot walk throught fs");
		umount_device(argv[1]);
		exit(EXIT_FAILURE);
	}

	printf("\nFTW completed");

	/*
	 * Setting up a pointer to target snapshot
	 */
	set_target_snapshot(snapshot_file);

	read_list();
	printf("\n Target : %s \n",target_snapshot->name);

	if(should_lvm_export)
		create_lvmexport();
	else {
		restore_fs();
		sprintf(command, "fsck.ext4dev -fxp %s", argv[1]);
		system(command);
	}

        exit(EXIT_SUCCESS);
}
