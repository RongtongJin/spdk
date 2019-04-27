/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/blobfs.h"
#include "spdk/bdev.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/log.h"
#include "spdk/string.h"

#include "spdk_internal/thread.h"
#include <fcntl.h>
#include <time.h>

#define WRITE_TIMES_PER_THREAD 400000
#define WRITE_THREAD_NUM 2
#define RAN_READ_THREAD_NUM 2
#define SEQ_READ_THREAD_NUM 2
#define RANDOM_READ_TIMES 400000
#define CACHE_SIZE 1024
#define MSG_SIZE 50

struct spdk_bs_dev *g_bs_dev;
const char *g_bdev_name;
struct spdk_filesystem *g_fs = NULL;
struct sync_args {
	struct spdk_fs_thread_ctx *channel;
};
pthread_t mSpdkTid;
volatile bool g_spdk_ready = false;
volatile bool g_spdk_start_failure = false;
uint32_t g_lcore = 0;
pthread_mutex_t lock;
uint64_t writePos=0;
uint32_t threadId=0;

__thread struct sync_args g_sync_args;

uint32_t spdk_env_get_first_core(void);

char * msgContent;

const char * fileName="benchmark-test";

static char *randstr(char *pointer, int n)
{
    int i,randnum;
    char str_array[63] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (i = 0; i < n; i++)
    {
        randnum = rand()%62;                    
        *pointer = str_array[randnum];          
        pointer++;
    }
    *pointer = '\0';                            
    return (pointer - n);                       
}

static void __call_fn(void *arg1, void *arg2)
{
	fs_request_fn fn;

	fn = (fs_request_fn)arg1;
	fn(arg2);
}

static void __send_request(fs_request_fn fn, void *arg)
{
	struct spdk_event *event;

	event = spdk_event_allocate(g_lcore, __call_fn, (void *)fn, arg);
	spdk_event_call(event);
}

static void
stop_cb(void *ctx, int fserrno)
{
	spdk_app_stop(0);
}

static void
shutdown_cb(void *arg1, void *arg2)
{
	struct spdk_filesystem *fs = arg1;
	// g_sync_args.channel = spdk_fs_alloc_io_channel_sync(g_fs);

	printf("done.\n");
	spdk_fs_unload(fs, stop_cb, NULL);
}

static void SpdkInitializeThread(void)
{
	struct spdk_thread *thread;

	if (g_fs != NULL) {
		pthread_mutex_lock(&lock);
		char threadName[20]={0};
		sprintf(threadName,"thread-%d",threadId++);
		thread = spdk_thread_create(threadName, NULL);
		spdk_set_thread(thread);
		g_sync_args.channel = spdk_fs_alloc_thread_ctx(g_fs);
		pthread_mutex_unlock(&lock);
	}
}

static void SpdkFinalizeThread(void)
{
	if (g_sync_args.channel) {
		spdk_fs_free_thread_ctx(g_sync_args.channel);
	}
}

static void fs_load_cb(__attribute__((unused)) void *ctx,
	   struct spdk_filesystem *fs, int fserrno)
{
	if (fserrno == 0) {
		g_fs = fs;
	}
	g_spdk_ready = true;
}

static void spdk_blobfs_run(__attribute__((unused)) void *arg1)
{
	struct spdk_bdev *bdev;

	bdev = spdk_bdev_get_by_name(g_bdev_name);

	if (bdev == NULL) {
		SPDK_ERRLOG("bdev %s not found\n", g_bdev_name);
		exit(1);
	}

	g_lcore = spdk_env_get_first_core();


	g_bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);
	printf("using bdev %s\n", g_bdev_name);
	// spdk_fs_init(g_bs_dev, NULL, __send_request, fs_load_cb, NULL);
	spdk_fs_load(g_bs_dev,__send_request, fs_load_cb, NULL);
}

static void * initialize_spdk(void *arg)
{
	struct spdk_app_opts *opts = (struct spdk_app_opts *)arg;
	int rc;

	rc = spdk_app_start(opts, spdk_blobfs_run, NULL);

	if (rc) {
		g_spdk_start_failure = true;
	} else {
		spdk_app_fini();
		free(opts);
	}
	pthread_exit(NULL);
}

static void* write_work(void *args){
	SpdkInitializeThread();
	struct spdk_file *file;
	int err=spdk_fs_open_file(g_fs,g_sync_args.channel,fileName,SPDK_BLOBFS_OPEN_CREATE,&file);
    if (err != 0) {
        SPDK_ERRLOG("open file on filesystem failed");
		exit(0);
    }
	printf("%s wirte start\n",spdk_thread_get_name(spdk_get_thread()));
	for(int i=0;i<WRITE_TIMES_PER_THREAD;i++){
		pthread_mutex_lock(&lock);
		spdk_file_write(file,g_sync_args.channel,msgContent,writePos,MSG_SIZE);
        spdk_file_sync(file,g_sync_args.channel);
    	writePos+=MSG_SIZE;
   		pthread_mutex_unlock(&lock);
	}
	printf("%s wirte done, writePos=%ld\n",spdk_thread_get_name(spdk_get_thread()),writePos);
	spdk_file_close(file,g_sync_args.channel);
	SpdkFinalizeThread();
	return NULL;
}

static void* random_read_work(void *args){
	SpdkInitializeThread();
	struct spdk_file *file;
	int err=spdk_fs_open_file(g_fs,g_sync_args.channel,fileName,SPDK_BLOBFS_OPEN_CREATE,&file);
    if (err != 0) {
        SPDK_ERRLOG("open file on filesystem failed");
		exit(0);
    }
	int readPos=0;
	uint64_t fileLength=spdk_file_get_length(file);
	char * line=(char *)malloc(MSG_SIZE);
	int readSize=0;
	int i=0;
	printf("%s random read start\n",spdk_thread_get_name(spdk_get_thread()));
	for(i=0;i<RANDOM_READ_TIMES;i++){
		readPos=(rand()%fileLength)-MSG_SIZE;
		if(readPos<0)
			readPos=0;
		readSize=spdk_file_read(file,g_sync_args.channel,line,readPos,MSG_SIZE);
		if(readSize<0){
			SPDK_NOTICELOG("random read size < 0\n");
			break;
		}
	}
	if(i==RANDOM_READ_TIMES)
		printf("%s random read done\n",spdk_thread_get_name(spdk_get_thread()));
	spdk_file_close(file,g_sync_args.channel);
	SpdkFinalizeThread();
	return NULL;
}

static void* seq_read_work(void *args){
	SpdkInitializeThread();
	struct spdk_file *file;
	int err=spdk_fs_open_file(g_fs,g_sync_args.channel,fileName,SPDK_BLOBFS_OPEN_CREATE,&file);
    if (err != 0) {
        SPDK_ERRLOG("open file on filesystem failed");
		exit(0);
    }
	char * line=(char *)malloc(MSG_SIZE+1);
	uint64_t readPos=0;
	uint64_t fileLength=spdk_file_get_length(file);
	int readSize=0;
	printf("%s sequential read start\n",spdk_thread_get_name(spdk_get_thread()));
	while(readPos<fileLength){
		readSize=spdk_file_read(file,g_sync_args.channel,line,readPos,MSG_SIZE);	
		if(readSize<0){
			SPDK_NOTICELOG("sequential read size < 0\n");
			break;
		}
		if(strcmp(line,msgContent)!=0){
			SPDK_ERRLOG("check error!!!\n");
			break;
		}
		readPos+=MSG_SIZE;
	}
	printf("%s sequential read done, readPos=%ld\n",spdk_thread_get_name(spdk_get_thread()),readPos);
	spdk_file_close(file,g_sync_args.channel);
	// struct spdk_file *file;
	// int err=spdk_fs_open_file(g_fs,g_sync_args.channel,fileName,SPDK_BLOBFS_OPEN_CREATE,&file);
    // if (err != 0) {
    //     SPDK_ERRLOG("open file on filesystem failed");
	// 	exit(0);
    // }
	// char * line=(char *)malloc(MSG_SIZE);
	// uint64_t readpos=0;
	// uint64_t fileLength=spdk_file_get_length(file);
	// printf("file size=%ld\n",fileLength);
	// int writeSize=0;
	// char fileName[50]={0};
	// sprintf(fileName,"checkfile-%s",spdk_thread_get_name(spdk_get_thread()));
	// int fd=open(fileName,O_CREAT|O_RDWR,0666);
	// if(fd<0){
	// 	SPDK_ERRLOG("fd < 0");
	// }
	// while(readpos<fileLength){
	// 	spdk_file_read(file,g_sync_args.channel,line,readpos,MSG_SIZE);	
	// 	writeSize=write(fd,line,MSG_SIZE);
	// 	if(writeSize<0){
	// 		SPDK_NOTICELOG("write checkfile size <0\n");
	// 		break;
	// 	}
	// 	readpos+=MSG_SIZE;
	// }
	// printf("write checkfile done\n");
	// free(line);
	// spdk_file_close(file,g_sync_args.channel);
	SpdkFinalizeThread();
	return NULL;
}

int main(int argc, char **argv)
{
	struct spdk_app_opts *opts = malloc(sizeof(struct spdk_app_opts));
	int rc = 0;

	if (argc < 3) {
		SPDK_ERRLOG("usage: %s <conffile> <bdevname>\n", argv[0]);
		exit(1);
	}

	spdk_app_opts_init(opts);
	opts->name = "blobfs-benchmark";
	opts->config_file = argv[1];
	// opts->reactor_mask = "0x01";
	opts->shutdown_cb = NULL;

	spdk_fs_set_cache_size(CACHE_SIZE);
	g_bdev_name = argv[2];
	
	pthread_create(&mSpdkTid, NULL, &initialize_spdk, opts);
	while (!g_spdk_ready && !g_spdk_start_failure)
		;
	if (g_spdk_start_failure) {
		free(opts);
		SPDK_ERRLOG("spdk_app_start() unable to run");
	}

	SpdkInitializeThread();
	
	if(!g_fs){
		SPDK_ERRLOG("g_fs is NULL\n");
        exit(0);
	}

	if(!g_sync_args.channel){
		SPDK_ERRLOG("g_sync_args.channel is NULL\n");
        exit(0);
	}

	printf("Generate a msgContent of random length\n");

	msgContent=(char *)malloc(MSG_SIZE);

	msgContent=randstr(msgContent,MSG_SIZE);

	printf("msgContent=%s\n",msgContent);

	/* if file exist, delete it*/
	struct spdk_file_stat stat;
	if(!spdk_fs_file_stat(g_fs,g_sync_args.channel,fileName,&stat))
		spdk_fs_delete_file(g_fs,g_sync_args.channel,fileName);

	pthread_mutex_init(&lock,NULL);

	/*wirte*/
    pthread_t *write_threads = (pthread_t *)malloc(sizeof(pthread_t)*WRITE_THREAD_NUM);
    if (write_threads == NULL)
    {
        SPDK_ERRLOG("malloc threads false;\n");
		exit(0);
    }
    memset(write_threads, 0, sizeof(pthread_t)*WRITE_THREAD_NUM);
	
	long write_start = clock();
    for(int i=0;i<WRITE_THREAD_NUM;i++){
		pthread_create(&write_threads[i],NULL,&write_work,NULL);
	}
	for(int i=0;i<WRITE_THREAD_NUM;i++){
		pthread_join(write_threads[i],NULL);
	}
    long write_end = clock();
	printf("Write done， write threads num = %d , cacheSize = %d, msgSize = %d, msgTotalNum = %d, cost time = %ld\n", 
	                    WRITE_THREAD_NUM, CACHE_SIZE, MSG_SIZE, WRITE_THREAD_NUM * WRITE_TIMES_PER_THREAD, write_end-write_start);
	free(write_threads);

	/*sequential read*/
	pthread_t *seq_read_threads = (pthread_t *)malloc(sizeof(pthread_t)*SEQ_READ_THREAD_NUM);
    if (seq_read_threads == NULL)
    {
        SPDK_ERRLOG("malloc threads false;\n");
		exit(0);
    }
    memset(seq_read_threads, 0, sizeof(pthread_t)*SEQ_READ_THREAD_NUM);
	
	long seq_read_start = clock();
    for(int i=0;i<SEQ_READ_THREAD_NUM;i++){
		pthread_create(&seq_read_threads[i],NULL,&seq_read_work,NULL);
	}
	for(int i=0;i<SEQ_READ_THREAD_NUM;i++){
		pthread_join(seq_read_threads[i],NULL);
	}
    long seq_read_end = clock();

	printf("Sequential read done， seq read threads num = %d, cacheSize = %d, msgSize = %d, msgTotalNum = %d, cost time = %ld\n", 
	                    SEQ_READ_THREAD_NUM, CACHE_SIZE, MSG_SIZE, SEQ_READ_THREAD_NUM * WRITE_THREAD_NUM * WRITE_TIMES_PER_THREAD, seq_read_end - seq_read_start);
	free(seq_read_threads);

	/*random read*/
	pthread_t *ran_read_threads = (pthread_t *)malloc(sizeof(pthread_t)*RAN_READ_THREAD_NUM);
    if (ran_read_threads == NULL)
    {
        SPDK_ERRLOG("malloc threads false;\n");
		exit(0);
    }
    memset(ran_read_threads, 0, sizeof(pthread_t)*RAN_READ_THREAD_NUM);
	
	long ran_read_start = clock();
    for(int i=0;i<RAN_READ_THREAD_NUM;i++){
		pthread_create(&ran_read_threads[i],NULL,&random_read_work,NULL);
	}
	for(int i=0;i<RAN_READ_THREAD_NUM;i++){
		pthread_join(ran_read_threads[i],NULL);
	}
    long ran_read_end = clock();

	printf("random read done， random read threads num = %d, random read time per thread = %d, msgSize = %d, msgTotalNum = %d, cost time = %ld\n", 
	                    RAN_READ_THREAD_NUM, RANDOM_READ_TIMES, MSG_SIZE, RAN_READ_THREAD_NUM * RANDOM_READ_TIMES, ran_read_end - ran_read_start);
	free(ran_read_threads);


	/*write to local filesystem for check*/
	// struct spdk_file *file;
	// int err=spdk_fs_open_file(g_fs,g_sync_args.channel,fileName,SPDK_BLOBFS_OPEN_CREATE,&file);
    // if (err != 0) {
    //     SPDK_ERRLOG("open file on filesystem failed");
	// 	exit(0);
    // }
	// char * line=(char *)malloc(MSG_SIZE);
	// uint64_t readpos=0;
	// uint64_t fileLength=spdk_file_get_length(file);
	// printf("file size=%ld\n",fileLength);
	// int writeSize=0;
	// int fd=open("checkfile",O_CREAT|O_RDWR,0666);
	// if(fd<0){
	// 	SPDK_ERRLOG("fd < 0");
	// }
	// while(readpos<fileLength){
	// 	spdk_file_read(file,g_sync_args.channel,line,readpos,MSG_SIZE);	
	// 	writeSize=write(fd,line,MSG_SIZE);
	// 	if(writeSize<0){
	// 		SPDK_NOTICELOG("write checkfile size <0\n");
	// 		break;
	// 	}
	// 	readpos+=MSG_SIZE;
	// }
	// printf("write checkfile done\n");
	// free(line);
	// spdk_file_close(file,g_sync_args.channel);
	
	/*clean up*/
	SpdkFinalizeThread();
	shutdown_cb(g_fs,NULL);
	return rc;
}
