#define FUSE_USE_VERSION 26

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fuse.h>
#include <fcntl.h>

#define FUSE_SRC_FILE "/home/Fs"
#define FILENAME_MAX_LENGTH 100
#define BLOCK_NUMBER 2048
#define BLOCK_SIZE 2048
#define FILE_NUMBER 64

typedef enum{ false, true} bool;

typedef struct fileMeta_struct {
	char name[FILENAME_MAX_LENGTH]; 
	int startBlock;			
	int size;			
	bool isDirectory;			
	bool isEmpty;			
} fileMeta;

	// block[i] >= 0   next block's index, 
	//          == -1  end,
	//          == -2  empty
typedef struct filesystem_struct{
	fileMeta meta[FILE_NUMBER];	
	int block[BLOCK_NUMBER];
} fileSystem;

fileSystem fs; //global FS

void initFS()
{
	int i = 0;
	while (i < sizeof(fs.meta) / sizeof(fs.meta[0])) 
	{
		memset(fs.meta[i].name, 0, FILENAME_MAX_LENGTH);
		fs.meta[i].isDirectory = false;
		fs.meta[i].isEmpty = true;
		fs.meta[i].startBlock = -1;
		fs.meta[i].size = 0;
		i++;
	}
	i = 0;
	for(i = 0; i < BLOCK_NUMBER - 1; i++)
	{
	   fs.block[i+1] = -2; //set "empty"
	}
}

void restoreFS()
{
  	FILE *f = fopen(FUSE_SRC_FILE, "r");
	fread(fs.meta, sizeof(fileMeta), FILE_NUMBER, f);
	fread(fs.block, sizeof(int), BLOCK_NUMBER, f);	
	int i;
	for(i = 0; i < BLOCK_NUMBER; i++)
	fs.block[i] = fs.block[i] == 0 ? -2 : fs.block[i];
	fclose(f);
}

fileMeta *getMetaAtIndex(int metaIndex) 
{
	return &fs.meta[metaIndex];
}

int writeBlocks(fileMeta *currentMeta) 
{
	FILE *f = fopen(FUSE_SRC_FILE, "r+");
	int i = currentMeta->startBlock;
	do {
		fseek(f, sizeof(fileMeta) * FILE_NUMBER + sizeof(int) * i, SEEK_SET);
		fwrite(&fs.block[i], sizeof(int), 1, f);
		i = fs.block[i];
	}
	while (i != -1);
	fclose(f);
	return 0;
}

int writeMeta(int metaIndex) 
{
	FILE *f = fopen(FUSE_SRC_FILE, "r+");
	fseek(f, metaIndex * sizeof(fileMeta), SEEK_SET);

	fileMeta* currentMeta = getMetaAtIndex(metaIndex);
	fwrite(currentMeta, sizeof(fileMeta), 1, f);

	fclose(f);
	return 0;
}

int firstEmptyMeta() 
{
	int i = 0;
	while (i < sizeof(fs.meta) / sizeof(fs.meta[0])) 
	{
		if (fs.meta[i].isEmpty) //FOUND!
			return i;
		i++;
	}
	return -1; 
}

int firstEmptyBlock() 
{
	int i = 0;
	while (i < BLOCK_NUMBER) 
	{
		if (fs.block[i] == -2) //FOUND!
			return i;
		i++;
	}
	return -1; 
}

int saveData(fileMeta *currentMeta, const char *data, int size, int offset) 
{
	if (size == 0) 
		return 0;

	FILE *f = fopen(FUSE_SRC_FILE, "r+");

	int i = currentMeta->startBlock;
	int j = offset / BLOCK_SIZE;
	while (j-- > 0) 
		i = fs.block[i];
	
	int left = size;
	int k = 0;
	int remain = offset % BLOCK_SIZE;

	int skip = sizeof(fileMeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER;

	while (left > 0) 
	{ 		
		if (remain + left > BLOCK_SIZE)
			k = BLOCK_SIZE - remain;
		else
			k = left;

		fseek(f, skip + i * BLOCK_SIZE + remain, SEEK_SET);
		fwrite(data + size - left, 1, k, f);

		if (remain + k == BLOCK_SIZE) 
		{
			if (fs.block[i] >= 0) i = fs.block[i];
			else 
			{
				if (firstEmptyBlock() == -1)
				 return -1;
				fs.block[i] = firstEmptyBlock();
				fs.block[firstEmptyBlock()] = -1;
				i = firstEmptyBlock();
			}
		}

		left = left - k;
		remain = 0;
	}
	currentMeta->size = size + offset;

	writeBlocks(currentMeta);
	fclose(f);
	return size;	
}

int addFile(char* fileName, int size, bool isDirectory) 
{
	fileMeta *meta = NULL;

	if (firstEmptyMeta() == -1) 
		return -1;

	int numEmpty = firstEmptyMeta();
	meta = getMetaAtIndex(numEmpty);
	if (firstEmptyBlock() == -1) 
		return -1;

	strcpy(meta->name, fileName);

	meta->startBlock = firstEmptyBlock();
	meta->size = size;
	meta->isDirectory = isDirectory;
	meta->isEmpty = false;

	int first = firstEmptyBlock();
	fs.block[first] = -1;
	writeMeta(numEmpty);
	writeBlocks(meta);

	return numEmpty;
}

int getData(fileMeta *currentMeta, char **data) 
{

	if (currentMeta == NULL) 
		return -1;

	FILE *f = fopen(FUSE_SRC_FILE, "r");
	char *buffer = (char *)malloc(currentMeta->size);
	int i = currentMeta->startBlock;
	int k = 0;
	int count;
	int skip = sizeof(fileMeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER;

	while (i != -1) 
	{
		int left = currentMeta->size - k * BLOCK_SIZE;

		if (left >= BLOCK_SIZE) 
			count = BLOCK_SIZE;
		else 
			count = left;
		fseek(f, skip + i * BLOCK_SIZE, SEEK_SET);
		fread(buffer + k * BLOCK_SIZE, 1, count, f);

		i = fs.block[i];
		k++;
	}
	fclose(f);
	*data = buffer;
	return currentMeta->size;
}

int createFileOrDirectory(const char* path, bool isDirectory) 
{
	fileMeta *meta;
	char *directory, *fileName, *data, *extData;

	fileName = strrchr(path, '/');

	if (fileName == NULL) 
	{
		strcpy(fileName, path);
		directory = (char*)malloc(2);
		strcpy(directory, "/\0");
	} 
	else 
	{
		fileName++;
		directory = (char *)malloc(strlen(path) - strlen(fileName) + 1);
		strncpy(directory, path, strlen(path) - strlen(fileName));
		directory[strlen(path) - strlen(fileName)] = '\0';
	}
	printf("Directory: %s fileName: %s\n", directory, fileName);
	extData = (char*)malloc(getData(meta, &data) + sizeof(int));

	int dataSize = getData(meta, &data);
	memcpy(extData, data, dataSize);

	int n = dataSize/sizeof(int);
	((int*)extData)[n] = addFile(fileName, 0, isDirectory);

	int resSize = dataSize + sizeof(int);
	saveData(meta, extData, resSize, 0);
	meta->size = resSize;	
	writeMeta(getMeta(directory, &meta));

	free(extData);
	free(directory);
	return 0;
}

int makeEmptyFS() 
{
	FILE *f = fopen(FUSE_SRC_FILE, "w+");
	
	char *bufMeta = (char *)malloc(sizeof(fileMeta)); //meta
	memset(bufMeta, '\0', sizeof(fileMeta));
	
	int i = 0;
	while (i < FILE_NUMBER) 
	{
		fwrite(bufMeta, sizeof(fileMeta), 1, f);
		i++;
	}	
	

	char *bufBlockInf = (char *)malloc(sizeof(int)); //blockInf
	memset(bufBlockInf, '\0', sizeof(int));
	
	i = 0;
	while (i < BLOCK_NUMBER) 
	{
		fwrite(bufBlockInf, sizeof(int), 1, f);
		i++;
	}
	
	char *bufBlockData = (char *)malloc(BLOCK_SIZE); //blockData
	memset(bufBlockData, '\0', BLOCK_SIZE);
	
	i = 0;
	while (i < BLOCK_NUMBER) 
	{
		fwrite(bufBlockData, BLOCK_SIZE, 1, f);
		i++;
	}
	
	fclose(f);

	initFS();
	addFile("/", 0, true);
	free(bufMeta);
	free(bufBlockInf);
	free(bufBlockData);
}

int removeFileOrDirectory(const char *path)
{
	int res = remove(path);
	return res != 0 ? -1 : 0;
}

char *getPathToDirectory(const char* path) 
{
	char *currentDirectory;
	char *p = strrchr(path, '/');

	if (p != NULL)
	 {
		int len = strlen(path) - strlen(p);
		if (len != 0) 
		{
			currentDirectory = (char *)malloc(len + 1);
			strncpy(currentDirectory, path, len);	
			currentDirectory[len] = '\0';
		} 
		else 
		{
			currentDirectory = (char *)malloc(2);
			strcpy(currentDirectory, "/\0");
		}
	} 
	else 
	{
		currentDirectory = (char *)malloc(2);
		strcpy(currentDirectory, "/\0");
	}
	return currentDirectory;
}

int remove(const char* path)
 {
	fileMeta *fileMeta, *dMeta;
	char *data, *extData;
	char *dir = getPathToDirectory(path);
	printf("Removed: Directory = %s\t Path = %s\n", dir, path);

	int dMetaNum = getMeta(dir, &dMeta);
	int fMetaNum = getMeta(path, &fileMeta);
	int size = getData(dMeta, &data);
	extData = (char *)malloc(size - sizeof(int));

	int i = 0, j = 0;
	while (i < size / sizeof(int)) 
	{
		//magic...
		((int *)extData)[j++] = ((int *)data)[i] != fMetaNum ? ((int *)data)[i] : ((int *)extData)[j++];
		i++; 
	}

	saveData(dMeta, extData, size, 0);
	dMeta->size = size - sizeof(int);
	writeMeta(dMetaNum);

	free(data);
	free(dir);
	return 0;
}
int getFileMetaIndex(char *data, char *fileName, int size) {
	int i = 0;
	while (i < size / sizeof(int)) {
		if (strcmp(fs.meta[((int *)data)[i]].name, fileName) == 0)
			return ((int *)data)[i];
		i++;	
	}
	return -1;
}

int getMeta(const char *path, fileMeta **meta) 
{
	char *fpath = (char*)malloc(strlen(path));
	strcpy(fpath, path);
	printf("%s\n", fpath);

	if (fpath && strcmp("/", fpath) == 0) { 
		//rootDir
		*meta = getMetaAtIndex(0); 	
		return 0;
	}

	fileMeta *m = NULL;
	char *p;

	p = fpath;

	if (*p++ == '/')
		m = getMetaAtIndex(0);
	else return -1;

	char *data, *s;
	char name[FILENAME_MAX_LENGTH];
	memset(name, '\0', FILENAME_MAX_LENGTH);

	int k = -1, size;
	
	while (p - fpath < strlen(fpath)) {
		if (m->size == 0)
			return -1;
		size = getData(m, &data);
		s = p;
		p = strchr(p, '/');
		if (p != NULL) {
			p = p + 1;
			strncpy(name, s, p - s - 1);			
		}
		else {
			strncpy(name, s, fpath + strlen(fpath) - s);
			p = fpath + strlen(fpath);		
		}
		k = getFileMetaIndex(data, name, size);
		if (k == -1) return -1;
		m = getMetaAtIndex(k);
		memset(name, '\0', FILENAME_MAX_LENGTH);
		free(data);
	}

	*meta = m;
	return k;
}

int openFile (const char *path)
{	
	fileMeta *meta;
	int metaIndex = getMeta(path, &meta);
	return metaIndex == -1 ? -1 : 0;
}

int readFile(fileMeta *currentMeta, char **buf, int size, int offset) 
{

	if (size == 0) 
		return 0;
	int s = currentMeta->size;
	if (offset > s) 
		return 0;
	if (size < s) 
		s = size;

	FILE *f = fopen(FUSE_SRC_FILE, "r");
	int i = currentMeta->startBlock;
	int j = offset / BLOCK_SIZE;
	while (j-- > 0) 
		i = fs.block[i];
	
	int left = s;
	int k = 0;
	int remain = offset % BLOCK_SIZE;
	char *tempData = (char *)malloc(s);
	int skip = sizeof(fileMeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER; 

	while (left > 0) 
	{ 	
		if (remain + left > BLOCK_SIZE)
			k = BLOCK_SIZE - remain;
		else
			k = left;

		fseek(f, skip + i * BLOCK_SIZE + remain, SEEK_SET);
		fread(tempData + s - left, 1, k, f);

		if (remain + k == BLOCK_SIZE) 
		{
			i = fs.block[i];
		}

		left = left - k;
		remain = 0;
	}

	fclose(f);
	*buf = (char *)tempData;
	return s;	
}

int read (const char *path, char *buf, size_t size, off_t offset)
{
	fileMeta *currentMeta;

	if (getMeta(path, &currentMeta) == -1) 
		return -ENOENT;

	char *data;
	int result = readFile(currentMeta, &data, size, offset);
	if (result == -1) 
		return -ENOENT;
	memcpy(buf, data, result);
    	return result;
}

int createFile(const char *path)
{
	return createFileOrDirectory(path, false) != 0 ? -1 : 0;
}

int createDirectory(const char *path)
{
	return createFileOrDirectory(path, true) != 0 ? -1 : 0;
}


//               ---FUSE---
static void *fs_init(struct fuse_conn_info *fi) 
{
	restoreFS();
}

static int fs_getattr(const char* path, struct stat *stbuf) 
{
	int res = 0;

	fileMeta *meta;
	if (getMeta(path, &meta) == -1)
		res = -ENOENT;

	memset(stbuf, 0, sizeof(struct stat));
    	if(meta->isDirectory) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
    	}
    	else {
        	stbuf->st_mode = S_IFREG | 0444;
        	stbuf->st_nlink = 1;
        	stbuf->st_size = meta->size;
    	}
	stbuf->st_mode = stbuf->st_mode | 0777;
    	return res;
}
static int fs_open(const char *path, struct fuse_file_info *fi)
{
	return openFile(path) == -1 ? -ENOENT : 0;
}
static int fs_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi) 
{
	return read(path, buf, size, offset);
}
static int fs_write(const char *path, const char *buf, size_t nbytes, 
			off_t offset, struct fuse_file_info *fi) 
{
	fileMeta *currentMeta;

	int metaIndex = getMeta(path, &currentMeta);

	if (metaIndex == -1) 
		return -ENOENT;
	if (saveData(currentMeta, buf, nbytes, offset) != -1) 
	{
		writeMeta(metaIndex);
		return saveData(currentMeta, buf, nbytes, offset);
	}
	return -1;
}
static int fs_create(const char *path, mode_t mode, struct fuse_file_info *finfo) 
{
	return createFile(path);
}
static int fs_mkdir(const char *path, mode_t mode)
{
	return createDirectory(path);
}
static int fs_rmdir(const char *path)
 {
	return removeFileOrDirectory(path);
}

static int fs_unlink(const char *path) 
{
	return removeFileOrDirectory(path);
}

//associating fuse functions with current realisations
static struct fuse_operations fuse_oper = 
{
        .getattr        = fs_getattr,
        .create         = fs_create,
        .unlink			= fs_unlink,
        .open           = fs_open,
        .read           = fs_read,
        .write          = fs_write,
		.mkdir          = fs_mkdir,
		.rmdir			= fs_rmdir,		
		.init  			= fs_init		
};

int main(int argc, char *argv[])
{
	printf("Starting...\n");
	if (argc > 1 && strcmp(argv[1], "new") == 0)
		makeEmptyFS();
	else		
	return fuse_main(argc, argv, &fuse_oper, NULL);
}
