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

#include <time.h>

#define WRITE_TIMES_PER_THREAD 10000
#define WRITE_THREAD_NUM 50
#define READ_THREAD_NUM 100

struct spdk_bs_dev *g_bs_dev;
const char *g_bdev_name;
struct spdk_filesystem *g_fs = NULL;
struct sync_args {
	struct spdk_io_channel *channel;
};
pthread_t mSpdkTid;
volatile bool g_spdk_ready = false;
volatile bool g_spdk_start_failure = false;
uint32_t g_lcore = 0;
pthread_mutex_t lock;
uint64_t writePos=0;



struct spdk_file *file;

__thread struct sync_args g_sync_args;

uint32_t spdk_env_get_first_core(void);

char * msgContent="12345678901234567890123456789012345678901234567890123456789012345678901234567890";

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
		thread = spdk_thread_create("benchmark");

		spdk_set_thread(thread);

		g_sync_args.channel = spdk_fs_alloc_io_channel_sync(g_fs);
	
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

static void spdk_blobfs_run(__attribute__((unused)) void *arg1,
		 __attribute__((unused)) void *arg2)
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

static void* write_work(void *arg){
	SpdkInitializeThread();
	for(int i=0;i<WRITE_TIMES_PER_THREAD;i++){
		pthread_mutex_lock(&lock);
		spdk_file_write(file,g_sync_args.channel,msgContent,0,strlen(msgContent));
        spdk_file_sync(file,g_sync_args.channel);
    	writePos+=sizeof(msgContent);
   		pthread_mutex_unlock(&lock);
	}
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
	opts->name = "spdk_mkfs";
	opts->config_file = argv[1];
	// opts->reactor_mask = "0x01";
	opts->shutdown_cb = NULL;

	spdk_fs_set_cache_size(512);
	g_bdev_name = argv[2];
	
	pthread_create(&mSpdkTid, NULL, &initialize_spdk, opts);
	while (!g_spdk_ready && !g_spdk_start_failure)
		;
	if (g_spdk_start_failure) {
		free(opts);
		SPDK_ERRLOG("spdk_app_start() unable to run");
	}

	int err=-1;
	
	SpdkInitializeThread();
	
	if(!g_fs){
		SPDK_ERRLOG("g_fs is NULL\n");
        exit(0);
	}
	if(!g_sync_args.channel){
		SPDK_ERRLOG("g_sync_args.channel is NULL\n");
        exit(0);
	}

    err=spdk_fs_open_file(g_fs,g_sync_args.channel,"helloworld2",SPDK_BLOBFS_OPEN_CREATE,&file);

    if (err != 0) {
        SPDK_ERRLOG("open file on filesystem failed");
    }
    // // spdk_file_truncate(file,g_sync_args.channel,0);
    // char * writeword="hello world";
    // spdk_file_write(file,g_sync_args.channel,writeword,0,strlen(writeword));
    // spdk_file_sync(file,g_sync_args.channel);
    
    // printf("file size=%ld\n",spdk_file_get_length(file));
    // int filesize=spdk_file_get_length(file);
    // char * readword=malloc(20);
    // spdk_file_read(file,g_sync_args.channel,readword,0,filesize);
    // printf("readword=%s\n",readword);
    // printf("read done\n");

    // spdk_file_close(file,g_sync_args.channel);

    // free(readword);
	// pthread_mutex_init(&lock,NULL);
    // long writeStart = clock();
    // pthread_t *read_threads = (pthread_t *)malloc(sizeof(pthread_t)*READ_THREAD_NUM);
    // if (read_threads == NULL)
    // {
    //     SPDK_NOTICELOG("malloc threads false;\n");
    // }
    // memset(read_threads, 0, sizeof(pthread_t)*READ_THREAD_NUM);
    // for(int i=0;i<READ_THREAD_NUM;i++){
	// 	pthread_create(&read_threads[i],NULL,&write_work,NULL);
	// }
	// for(int i=0;i<READ_THREAD_NUM;i++){
	// 	pthread_join(read_threads[i],NULL);
	// }
    // long writeEnd = clock();
	// printf("cost time = %ld",writeEnd-writeStart);
	// free(read_threads);
	spdk_file_close(file,g_sync_args.channel);
	shutdown_cb(g_fs,NULL);
	return rc;
}
