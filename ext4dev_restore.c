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

#include <ftw.h>
#include "e2p/e2p.h"

#define SNAPSHOT_SHIFT 4096*1036
#define MAX 50
#define MAX_FILE_NAME 1024

struct snapshot_list {
	char name[MAX_FILE_NAME];
	int id;
	struct snapshot_list *next;
} *list_head;

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
		list_node->next = NULL;
		list_head = list_node;
	}
	else if (list_head->next == NULL) {
		if(list_head->id < id) {
			list_node = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
			strcpy(list_node->name, fpath);
			list_node->id = id;
			list_node->next = NULL;

			list_head->next = list_node;
		}
		else {
			list_node = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
			strcpy(list_node->name, fpath);
			list_node->id = id;

			list_node->next = list_head;
			list_head = list_node;
		}
	}
	else {
		if(id < list_head->id) {
			list_node = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
			strcpy(list_node->name, fpath);
			list_node->id = id;
			list_node->next = list_head;
			list_head = list_node;			
		}
		else {
			struct snapshot_list *temp;
			list_node = list_head;
			while(list_node->next && (list_node->next->id < id))
				list_node = list_node->next;
			temp = (struct snapshot_list *)malloc(sizeof(struct snapshot_list));
			strcpy(temp->name, fpath);
			temp->id = id;
			temp->next = list_node->next;
			list_node->next = temp;
		}
	}
	return 0;
}	

int read_list(void)
{
	struct snapshot_list *node = list_head;

	while(node != NULL) {
		printf("\n%s", node->name);
		node = node->next;
	}

	return 0;
}

struct fiemap *read_fiemap(int fd)
{
        struct fiemap *fiemap;
        int extents_size;

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

void dump_fiemap(struct fiemap *fiemap, int snapshot_file, int disk_image)
{
	int i;
	void *buf;
	
	for (i=0;i<fiemap->fm_mapped_extents;i++) {
		if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
		   fiemap->fm_extents[i].fe_physical){
			buf = (void *)malloc(fiemap->fm_extents[i].fe_length);
			
			lseek(snapshot_file, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			read(snapshot_file, buf, fiemap->fm_extents[i].fe_length);			
			
			lseek(disk_image, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			write(disk_image, buf, fiemap->fm_extents[i].fe_length);
			
			free(buf);
		}
	}
}

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

void umount_and_exit(char *device, int status)
{
	umount(device);
	rmdir("/mnt/temp");
	exit(status);
}

int main(int argc, char **argv)
{
        int fd, disk_image;
	char command[MAX], mount_point[MAX], snapshot_dir[MAX], snapshot_file[MAX];
	char *snapshot_name;
	struct snapshot_list *node;
	struct fiemap *fiemap;

	list_head = NULL;

        if (argc != 3) {
		printf("\next4dev_restore <device> <snapshot>\n");
                exit(EXIT_FAILURE);
        }

	if(mkdir("/mnt/temp",S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
		printf("\nFailed to create mount point");
		exit(EXIT_FAILURE);
	}
	strcpy(mount_point, "/mnt/temp");

	sprintf(command,"snapshot.ext4dev mount %s %s",argv[1],mount_point);
	system(command);

	sprintf(snapshot_dir, "%s/.snapshots", mount_point);
	sprintf(snapshot_file, "%s/%s", snapshot_dir, argv[2]);

	if(ftw(snapshot_dir, add_to_snapshot_list, 10) != 0) {
		printf("\nCannot walk throught fs");
		umount_and_exit(argv[1], EXIT_FAILURE);
	}

	printf("\nFTW completed");
	read_list();
	disk_image = open("/tmp/disk_image.img", O_CREAT | O_WRONLY, 0777);

	if(list_head == NULL) {
		printf("\nNo snapshot taken");
		umount_and_exit(argv[1], EXIT_FAILURE);
	}

	node = list_head;

	do {
		snapshot_name = node->name + strlen(snapshot_dir) + 1;
		sprintf(command, "snapshot.ext4dev enable %s", snapshot_name);
		printf("\n%s\n",command);
		system(command);
		
		if ((fd = open(node->name, O_RDONLY)) < 0) {
			fprintf(stderr, "Cannot open file %s\n", node->name);
			umount_and_exit(argv[1], EXIT_FAILURE);
		}
		else if ((fiemap = read_fiemap(fd)) != NULL) 
			dump_fiemap(fiemap, fd, disk_image);
		else {
			printf("\nfiemap ioctl failed");
			umount_and_exit(argv[1], EXIT_FAILURE);
		}
		close(fd);
		node = node->next;
		sprintf(command,"%s/%s",snapshot_dir,snapshot_name);
		printf("com : %s \t file : %s\n",command,snapshot_file);
	} while(strcmp(command, snapshot_file));
	
	close(disk_image);
				
	strcpy(command, "umount ");
	strcat(command, mount_point);
	system(command);

	restore_fs(argv[1]);

	sprintf(command, "fsck.ext4dev -fxy %s", argv[1]);
	system(command);

	rmdir("/mnt/temp");
        exit(EXIT_SUCCESS);
}
