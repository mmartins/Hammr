/*
 * =====================================================================================
 *
 *       Filename:  scc.c
 *
 *    Description:  RCCE main interface
 *
 *        Version:  1.0
 *        Created:  10/28/2011 06:46:31 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Marcelo Martins (martins), martins@cs.brown.edu
 *        Company:  Brown University
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>

#include <jni.h>	/* Java Native Interface headers */
#include "include/SCC.h"	/* Java glue */
#include "RCCE.h"

typedef volatile unsigned char* t_vcharp;

#define CRB_OWN		0xf8000000
#define MYTITLEID	0x100

JNIEXPORT jint JNICALL
Java_SCC_init(JNIEnv *env, jobject *obj)
{
	int argc = 0;
	char **args = NULL;
	
	/* RCCE initialization asks for argc and argv of calling program. Since
	 * call from library, we shouldn't care about it.
	 */
	ret = RCCE_init(&argc, &args);

	return ret;
}

JNIEXPORT jint JNICALL
Java_SCC_terminate(JNIEnv *env, jobject *obj)
{
	return RCCE_finalize();
}

JNIEXPORT jint JNICALL
Java_SCC_getTileSize(JNIENV *env, jobject *obj)
{
	return RCCE_num_ue();
}

JNIEXPORT jint JNICALL
Java_SCC_getNodeRank(JNIENV *env, jobject *obj)
{
	return RCCE_ue();
}

/*
 * Adapted from readTileID.c (SCC Programming Guide)
 *
 * Return information on SCC tile
 */
JNIEXPORT jint JNICALL
Java_SCC_getTileInfo(JNIENV *env, jobject *obj)
{
	jfieldID fid;
	jclass cls;

	/* NCMDeviceFD is the file descriptor for non-cacheable memory
	 * (e.g., config regs.
	 */
	int result, PAGE_SIZE, NCM_device_fd;

	unsigned int result, tileID, coreID, x_val, y_val;
	unsigned int coreID_mask = 0x00000007, x_mask = 0x00000078, y_mask = 0x00000780;

	t_vcharp mapped_addr;
	unsigned int aligned_addr, page_offset, config_addr;

	cls = (*env)->GetObjectClass(env, obj);

	config_addr = CRB_OWN + MYTITLEID;
	PAGE_SIZE = getpagesize();

	if (NCM_device_fd = open("/dev/rckncm", O_RDWR|O_SYNC) < 0) {
		perror("open");
		return -1;
	}

	aligned_addr = config_addr & (~(PAGE_SIZE-1));
	page_offset = config_addr - aligned_addr;

	mapped_addr = (t_vcharp) mmap(NULL, PAGE_SIZE, PROT_WRITE|PROT_READ,
								MAP_SHARED, NCM_device_fd, aligned_add);

	if (mapped_addr == MAP_FAILED){
		perror("mmap");
		return -1;
	}

	result = *(unsigned int *)(mapped_adrr + page_offset);
	munmap((void *)mapped_addr, PAGE_SIZE);

	coreID = result & coreID_mask;
	x_val = (result & x_mask) >> 3;
	y_val = (result & y_mask) >> 7;
	tileID = y_val * 16 + x_val;

	/* Put the results back to Java.
	 * Get the ID of the Java class member variable newFreqDiv (which is
	 * of type int, hence the "I" signature */
	fid = (*env)->GetFieldID(env, cls, "coreID", "I");
	(*env)->SetIntField(env, obj, fid, coreID);
	fid = (*env)->GetFieldID(env, cls, "coordX", "I");
	(*env)->SetIntField(env, obj, fid, x_val);
	fid = (*env)->GetFieldID(env, cls, "coordY", "I");
	(*env)->SetIntField(env, obj, fid, y_val);
	fid = (*env)->GetFieldID(env, cls, "tileID", "I");
	(*env)->SetIntField(env, obj, fid, tileID);

	return result;
}
