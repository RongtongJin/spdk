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

struct spdk_bs_dev *g_bs_dev;
const char *g_bdev_name;
static uint64_t g_cluster_size;
struct spdk_filesystem *g_fs = NULL;
struct sync_args {
	struct spdk_io_channel *channel;
};
pthread_t mSpdkTid;
volatile bool g_spdk_ready = false;
volatile bool g_spdk_start_failure = false;
uint32_t g_lcore = 0;

__thread struct sync_args g_sync_args;


void __call_fn(void *arg1, void *arg2)
{
	fs_request_fn fn;

	fn = (fs_request_fn)arg1;
	fn(arg2);
}

void __send_request(fs_request_fn fn, void *arg)
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

void SpdkInitializeThread(void)
{
	struct spdk_thread *thread;

	if (g_fs != NULL) {
		thread = spdk_thread_create("spdk_blobfs");

		spdk_set_thread(thread);

		g_sync_args.channel = spdk_fs_alloc_io_channel_sync(g_fs);
	
	}
}


void fs_load_cb(__attribute__((unused)) void *ctx,
	   struct spdk_filesystem *fs, int fserrno)
{
	if (fserrno == 0) {
		g_fs = fs;
	}
	g_spdk_ready = true;
}

void spdk_blobfs_run(__attribute__((unused)) void *arg1,
		 __attribute__((unused)) void *arg2)
{
	struct spdk_bdev *bdev;

	bdev = spdk_bdev_get_by_name(g_bdev_name);

//	SPDK_NOTICELOG("thread name =%s\n",spdk_thread_get_name(spdk_get_thread()));

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

void * initialize_spdk(void *arg)
{
	struct spdk_app_opts *opts = (struct spdk_app_opts *)arg;
	int rc;

	rc = spdk_app_start(opts, spdk_blobfs_run, NULL);
	/*
	 * TODO:  Revisit for case of internal failure of
	 * spdk_app_start(), itself.  At this time, it's known
	 * the only application's use of spdk_app_stop() passes
	 * a zero; i.e. no fail (non-zero) cases so here we
	 * assume there was an internal failure and flag it
	 * so we can throw an exception.
	 */
	if (rc) {
		g_spdk_start_failure = true;
	} else {
		spdk_app_fini();
		free(opts);
	}
	pthread_exit(NULL);

}

int main(int argc, char **argv)
{
	struct spdk_app_opts *opts = malloc(sizeof(struct spdk_app_opts));
	printf("--------------------1\n");
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

	SpdkInitializeThread();

	int err=-1;
	
	if(!g_fs){
		SPDK_ERRLOG("g_fs is NULL\n");
	}
	if(!g_sync_args.channel){
		SPDK_ERRLOG("g_sync_args.channel is NULL\n");
	}
	if(g_sync_args.channel!=NULL){
		struct spdk_file *file;
		err=spdk_fs_open_file(g_fs,g_sync_args.channel,"helloworld",SPDK_BLOBFS_OPEN_CREATE,&file);
		// printf("--------------------6\n");
		if (err != 0) {
			SPDK_ERRLOG("open file on filesystem failed");
		}
		char * writeword="hello world";
		spdk_file_write(file,g_sync_args.channel,writeword,0,strlen(writeword));
		spdk_file_sync(file,g_sync_args.channel);
		printf("file size=%ld\n",spdk_file_get_length(file));
		int filesize=spdk_file_get_length(file);
		char * readword=malloc(20);
		spdk_file_read(file,g_sync_args.channel,readword,0,filesize);
		printf("readword=%s\n",readword);
		spdk_file_close(file,g_sync_args.channel);
		free(readword);
	}
	shutdown_cb(g_fs,NULL);
	return rc;
}
