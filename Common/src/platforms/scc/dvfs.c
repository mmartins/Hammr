/*
 * =====================================================================================
 *
 *       Filename:  dvfs.c
 *
 *    Description:  SCC DVFS control interface
 *
 *        Version:  1.0
 *        Created:  10/28/2011 05:26:47 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Marcelo Martins (martins), martins@cs.brown.edu
 *        Company:  Brown University
 *
 * =====================================================================================
 */

#include <jni.h>					/* Java Native Interface headers */
#include "include/RCCE.h"
#include "include/RCCE_lib_pwr.h"	/* Support for SCC Power API */
#include "include/SCC_DVFS.h"		/* Java glue */

/* 
 *
 * Tile Frequency (MHz) : RCCE Frequency Divider
 *
 *	800 : 2
 *	533 : 3
 *	400 : 4
 *	320 : 5
 *	266 : 6
 *	228 : 7
 *	200 : 8
 *	178 : 9
 *	160 : 10
 *	145 : 11
 *	133 : 12
 *	123 : 13
 *	114 : 14
 *	106 : 15
 *	100 : 16
 *
 */

int setPower(int freqDiv, RCCE_REQUEST *req, int *freqDiv_new, int *voltLevel_new);

static int
setPower(int freqDiv, RCCE_REQUEST *req, int *freqDiv_new, int *voltLevel_new)
{
	RCCE_REQUEST request;
	return RCCE_iset_power(freqDiv, req, freqDiv_new, voltLevel_new);
}

JNIEXPORT jint JNICALL
Java_SCC_DVFS_getPowerDomainSize(JNIEnv *env, jobject *obj)
{
	return RCCE_power_domain_size();
}

JNIEXPORT jint JNICALL
Java_SCC_DVFS_getPowerDomain(JNIENV *env, jobject *obj)
{
	return RCCE_power_domain();
}

JNIEXPORT jint JNICALL
Java_SCC_DVFS_getPowerDomainMaster(JNIENV *env, jobject *obj)
{
	return RCCE_power_domain_master();
}

JNIEXPORT jint JNICALL
Java_SCC_DVFS_getPowerDomainSize(JNIENV *env, jobject *obj)
{
	return RCCE_power_domain_size();
}

JNIEXPORT jdouble JNICALL
Java_SCC_DVFS_setPower(JNIEnv *env, jobject obj, jint freqDiv)
{
	int new_freq, new_volt;
	jfieldID fid;
	jclass cls;
	RCCE_REQUEST request;

	cls = (*env)->GetObjectClass(env, obj);
	result = setPower((int)freeDiv, &request, &new_freq, &new_volt);

	/* Put the results back to Java.
	 * Get the ID of the Java class member variable newFreqDiv (which is
	 * of type int, hence the "I" signature */
	fid = (*env)->GetFieldID(env, cls, "newFreqDiv", "I");
	(*env)->SetIntField(env, obj, fid, new_freq);
	fid = (*env)->GetFieldID(env, cls, "newVoltLevel", "I");
	(*env)->SetIntField(env, obj, fid, new_volt);

	return result;
}

JNIEXPORT jint JNICALL
Java_SCC_DVFS_waitPower(JNIENV *env, jobject *obj)
{
	RCCE_REQUEST request;
	RCCE_wait_power(&request);
}

JNIEXPORT jint JNICALL
Java_SCC_DVFS_setFrequencyDivider(JNIENV *env, jobject *obj, jint freqDiv)
{
	int new_freq;
	jfieldID fid;
	jclass cls;

	cls = (*env)->GetObjectClas(env, obj);
	result = RCCE_set_frequency_divider(freqDiv, &new_freq);
	fid = (*env)->GetFieldID(env, cls, "newFreqDiv", "I");
	(*env)->SetIntFieldID(env, obj, fid, new_freq);

	return result;
}
