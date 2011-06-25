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

#define SNAPSHOT_SHIFT 4096*1036
#define MAX 50

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

void restore_fs(struct fiemap *fiemap, char *device_path, int disk_image)
{
	int i;
	int device=open("/dev/sda4", O_RDWR, 0660);
	void *buf;

	for (i=0;i<fiemap->fm_mapped_extents;i++) {
		if(fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT !=
		   fiemap->fm_extents[i].fe_physical){

			buf = (void *)malloc(fiemap->fm_extents[i].fe_length);

			lseek(disk_image, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			read(disk_image, buf, fiemap->fm_extents[i].fe_length);

			lseek(device, fiemap->fm_extents[i].fe_logical - SNAPSHOT_SHIFT, SEEK_SET);
			write(device, buf, fiemap->fm_extents[i].fe_length);

			free(buf);
		}
	}
	close(device);
}

int main(int argc, char **argv)
{
        int fd, disk_image;
	char command[MAX];

        if (argc != 3) {
		printf("\next4dev_restore <device> <path_of_snapshot>\n");
                exit(EXIT_FAILURE);
        }

	disk_image = open("/tmp/disk_image.img", O_CREAT | O_WRONLY, 0777);

	if ((fd = open(argv[2], O_RDONLY)) < 0) {
		fprintf(stderr, "Cannot open file %s\n", argv[2]);
	}
	else {
		struct fiemap *fiemap;

		if ((fiemap = read_fiemap(fd)) != NULL) 
			dump_fiemap(fiemap, fd, disk_image);
		close(fd);
		close(disk_image);
		disk_image = open("/tmp/disk_image.img", O_RDONLY);

		strcpy(command, "umount ");
		strcat(command, argv[1]);
		system(command);

		restore_fs(fiemap, argv[1], disk_image);
	}
	close(disk_image);
        exit(EXIT_SUCCESS);
}
