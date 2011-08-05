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
#define SECTOR_SIZE 4096

/*
 * List of snapshots sorted according to their IDs
 */

struct snapshot_list {
	char name[MAX_FILE_NAME];
	int id;
	struct snapshot_list *next;
	struct snapshot_list *prev;
} *list_head, *target_snapshot;

struct lvm_export {
	unsigned long *area;
	struct lvm_export *next;
} *lvm_export;

struct disk_exception {
    uint64_t old_chunk;
    uint64_t new_chunk;
};

char snapshot_dir[MAX], mount_point[MAX];

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
 * Helper function for LVM_export
 * Function reads all metadata chunks
 */
int read_lvm_export_metadata(void)
{
	struct lvm_export *p=lvm_export;
	unsigned long offset = 0, data_in_chunk;
	int done = 0;
	
	printf("\nMetadata Chunk : ");
	while(!done) {
		data_in_chunk = *(unsigned long *)(p->area+offset);
		if(data_in_chunk == -1)
			done = 1;
		else
			printf(" %ld ", data_in_chunk);
		offset = (offset + 1) % SECTOR_SIZE;
		if(!offset)
			p=p->next;
	}
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

int create_lvmexport(void)
{
	struct fiemap *fiemap;
	int fd, lvm_image, offset=0, mdata_blocks=1;
	int i, j, k, location;
	char command[MAX], *snapshot_name;
	struct snapshot_list *p;
	uint64_t *exception_mdata;
	struct disk_exception de;

	p = target_snapshot;

	exception_mdata = (uint64_t *)malloc(SECTOR_SIZE);
	memset(exception_mdata, -1, SECTOR_SIZE);

	//	lvm_image = open(LVM_IMAGE_PATH, O_WRONLY|O_CREAT);

	while(p) {
		if ((fd = open(p->name, O_RDONLY)) < 0) {
			fprintf(stderr, "Cannot open file %s\n", p->name);
			return 0;
		}	
		
		snapshot_name = p->name + strlen(snapshot_dir) + 1;
		sprintf(command, "snapshot.ext4dev enable %s", snapshot_name);
		system(command);
		
		offset = 0;
		fiemap = read_fiemap(fd);

		for (i=0;i<fiemap->fm_mapped_extents;i++) {
			if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
			   fiemap->fm_extents[i].fe_physical) {

				for(j=0,k=0; j < (fiemap->fm_extents[i].fe_length) / SECTOR_SIZE; j++,k+=2) {
					location = search_in_area(exception_mdata, 2*(offset+j),
								  (fiemap->fm_extents[i].fe_logical 
								   - SNAPSHOT_SHIFT)/SECTOR_SIZE + j);
					printf("\nSearching for %ld",   (fiemap->fm_extents[i].fe_logical 
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
						de.new_chunk = 0;
						exception_mdata[2*offset+k] = de.old_chunk;
						exception_mdata[2*offset+k+1] = de.new_chunk;
						printf("Writing at %d,%d\n", 2*offset+k,2*offset+k+1);
					}
					else {
						printf("\nFound\n");
					}
					/*
					 * Block already present
					 * Just overwrite the corresponding data chunk
					 */
				}
				offset = offset +(fiemap->fm_extents[i].fe_length/SECTOR_SIZE);
				
				/*
				 * offset might exceed SECTOR_SIZE
				 * in that case chunks are full
				 * allocate one more chunk
				 */
				if(offset % (SECTOR_SIZE * mdata_blocks) == 0) {
					uint64_t *temp;
					
					temp = (uint64_t *)malloc(SECTOR_SIZE * (mdata_blocks+1));
					memcpy(temp, exception_mdata, SECTOR_SIZE*mdata_blocks);
					free(exception_mdata);
					exception_mdata = temp;
					mdata_blocks++;
				}
	       		}
		}
		p = p->prev;
		close(fd);
	}

	printf("\nNumber of metadata blocks : %d", mdata_blocks);
	printf("\nException Store\n");
	for(i=0; exception_mdata[i] != -1 ;i+=2) {
		printf("\n%ld\t%ld\n", exception_mdata[i], exception_mdata[i+1]);
	}
	return 0;
}
	
/*
 * The funtion either dumps fiemap to temporary disk image or dumps it to
 * lvm export list depending upon the value of should_lvm_export
 * if set builds lvm export list
 * if reset builds temporary disk image
 */
void dump_fiemap(struct fiemap *fiemap, int snapshot_file, int disk_image, int should_lvm_export)
{
	int i,j,k;
	void *buf;
	static int full=0;
	static unsigned long offset=0;
	static struct lvm_export *p=NULL;	

	if(!p && should_lvm_export)
		p = lvm_export;

	for (i=0;i<fiemap->fm_mapped_extents;i++) {
		if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
		   fiemap->fm_extents[i].fe_physical) {
			buf = (void *)malloc(fiemap->fm_extents[i].fe_length);
			
			lseek(snapshot_file, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			read(snapshot_file, buf, fiemap->fm_extents[i].fe_length);			

			if(!p && should_lvm_export) {
				/*
				 * Allocating for the first time
				 * Allocate area for 1 metadata chunk + data chunks addressable from that chunk
				 */
				p = (struct lvm_export *)malloc(sizeof(struct lvm_export *));
				p->area = malloc(((SECTOR_SIZE>>4)+1) * SECTOR_SIZE);
				printf("\nAllocating %d bytes\n", (((SECTOR_SIZE>>4)+1) * SECTOR_SIZE));
				p->next = NULL;
				memset(p->area, -1,  ((SECTOR_SIZE>>4)+1) * SECTOR_SIZE);
				lvm_export = p;
			}
			else if(full && should_lvm_export) {
				/*
				 * Chunks full
				 * Need to allocate another metdata chunk + data chunks
				 */
				p->next = (struct lvm_export *)malloc(sizeof(struct lvm_export *));
				p = p->next;
				p->area = malloc(((SECTOR_SIZE>>4)+1) * SECTOR_SIZE);
				p->next = NULL;
				memset(p->area, -1, ((SECTOR_SIZE>>4)+1) * SECTOR_SIZE);
				full = 0;
			}
	
			if(should_lvm_export) {
				/*
				 * Building LVM export list
				 */
				int location = 0;
				struct disk_exception de;

				for(j=0,k=0; j < (fiemap->fm_extents[i].fe_length) / SECTOR_SIZE; j++,k+=2) {
					location = search_in_area(p->area, offset+j,
								  (fiemap->fm_extents[i].fe_logical 
								   - SNAPSHOT_SHIFT)/SECTOR_SIZE + k);

					if(location < 0) {
						/*
						 * The is not present in export list
						 * Writing metadata to metadata chunk
						 */

						de.old_chunk 
							= fiemap->fm_extents[i].fe_logical/SECTOR_SIZE
							+ j - SNAPSHOT_SHIFT/SECTOR_SIZE;
						de.new_chunk = 0;
						p->area[2*offset+k] = de.old_chunk;
						p->area[2*offset+k+1] = de.new_chunk;

						/*
						 * Writing data to data chunks
						 */
						printf("\nCopying %ld bytes at %ld\n", (long)fiemap->fm_extents[i].fe_length, (long)((offset+j+1)*SECTOR_SIZE));
						memcpy(p->area + (offset+j+1)*SECTOR_SIZE, buf,
						       SECTOR_SIZE);
					}
					else{}
						/*
						 * Block already present
						 * Just overwrite the corresponding data chunk
						 */
					/*	memcpy(p->area + (location + 2)*SECTOR_SIZE, buf,
						fiemap->fm_extents[i].fe_length);*/
				}
				offset = (offset + (fiemap->fm_extents[i].fe_length/SECTOR_SIZE))
					% SECTOR_SIZE;
				/*
				 * offset might exceed sectorsize
				 * in that case chunks are full
				 * set flag full
				 */
				if(!offset)
					full = 1;
			}
			else {
				/*
				 * Reverting to earlier snapshot
				 */
				lseek(disk_image, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
				write(disk_image, buf, fiemap->fm_extents[i].fe_length);
			}
			free(buf);
		}
	}
}
/*
 * search_in_area searches for key in area from start to end
 */
int search_in_area(uint64_t *area, int end, uint64_t key)
{
	int start=0, location;
	
	while(start<=end) {
		location = (start+end)/2;
		if(key == area[location*2])
			return location;
		else if(key < area[location*2])
			end = location - 2;
		else
			start = location + 2;
	}

	/*	printf("\nKey = %ld Traversing : ", key);
	while((start<=end) && (area[start] != key)) {
		printf(" %ld ", area[start]);
		start += 2;
	}
	printf("\n");*/

	if(start <= end)
		return start;
	return -1;
}

/*
 * Restores the device to the earlier snapshot.
 * Function assumes that the required disk image is created at /tmp/disk_image.img
 */
void restore_fs(char *device_path)
{
	int i;
	int device=open(device_path, O_RDWR, 0660), disk_image=open("/tmp/disk_image.img", O_RDONLY);
	void *buf;
	struct fiemap *fiemap;

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
		printf("\nCannot walk throught fs");
		return error;
	}

	error = umount(device);
	if(error)
		return error;
	rmdir(MNT);
	return error;
}

int main(int argc, char **argv)
{
        int fd, error, disk_image, should_lvm_export=0;
	char command[MAX], snapshot_file[MAX];
	char *snapshot_name;
	struct snapshot_list *node;
	struct fiemap *fiemap;

	list_head = NULL;
	lvm_export = NULL;

        if (argc < 4) {
		printf("\next4dev_restore <device> <snapshot> [lvmexport/revert]\n");
                exit(EXIT_FAILURE);
        }

	/*
	 * Should export to LVM ?
	 */
	if(argc == 4 && !strcmp(argv[3],"lvmexport"))
		should_lvm_export = 1;

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

	set_target_snapshot(snapshot_file);

	read_list();
	printf("\n Target : %s \n",target_snapshot->name);

	if(should_lvm_export) {
		create_lvmexport();
		return 0;
	}
	disk_image = creat("/tmp/disk_image.img", 0777);
	
	if(list_head == NULL) {
		printf("\nNo snapshot taken");
		umount_device(argv[1]);
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

		sprintf(command, "snapshot.ext4dev enable %s", snapshot_name);
		printf("\n%s\n",command);
		system(command);
		
		if ((fd = open(node->name, O_RDONLY)) < 0) {
			fprintf(stderr, "Cannot open file %s\n", node->name);
			umount_device(argv[1]);
			exit(EXIT_FAILURE);
		}
		else if ((fiemap = read_fiemap(fd)) != NULL)
			dump_fiemap(fiemap, fd, disk_image, should_lvm_export);
		else {
			printf("\nfiemap ioctl failed");
			umount_device(argv[1]);
			exit(EXIT_FAILURE);
		}
		close(fd);
		node = node->next;
		sprintf(command,"%s/%s",snapshot_dir,snapshot_name);
		printf("com : %s \t file : %s\n",command,snapshot_file);
	} while(strcmp(command, snapshot_file));
	
	close(disk_image);
				
	if(umount_device(mount_point)) {
		printf("\nError while umounting");
		exit(EXIT_FAILURE);
	}
	if(!should_lvm_export) {
		restore_fs(argv[1]);

		sprintf(command, "fsck.ext4dev -fxy %s", argv[1]);
		system(command);
	}
	else
		read_lvm_export_metadata();
        exit(EXIT_SUCCESS);
}
