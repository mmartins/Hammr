/*
 * =====================================================================================
 *
 *       Filename:  dvfs.c
 *
 *    Description:  cpufreq interface to x86 processors
 *
 *        Version:  1.0
 *        Created:  11/27/2011 08:59:56 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Marcelo Martins (martins), martins@cs.brown.edu
 *        Company:  Brown University
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>

#include <jni.h>	/* Java Native Interface Headers */
#include "platforms_x86_X86_DVFS.h"	/* Java glue */
#include <cpufreq.h>

JNIEXPORT jlong JNICALL Java_platforms_x86_X86_1DVFS_getTransitionLatency
  (JNIEnv *env, jobject obj, jint cpu)
{
	return (jlong) cpufreq_get_transition_latency((unsigned long)cpu);
}

JNIEXPORT jint JNICALL Java_platforms_x86_X86_1DVFS_getHardwareLimits
  (JNIEnv *env, jobject obj, jint cpu)
{
	unsigned long min, max;
	jclass cls;
	jfieldID fid;
	int ret;

	ret = cpufreq_get_hardware_limits((unsigned int)cpu, &min, &max);

	if (ret < 0)
		return ret;

	cls = (*env)->GetObjectClass(env, obj);

	fid = (*env)->GetFieldID(env, cls, "minFreq", "J");
	(*env)->SetLongField(env, obj, fid, min);
	fid = (*env)->GetFieldID(env, cls, "maxFreq", "J");
	(*env)->SetLongField(env, obj, fid, max);

	(*env)->DeleteLocalRef(env, cls);
	return ret;
}

JNIEXPORT jstring JNICALL Java_platforms_x86_X86_1DVFS_getGovernor
  (JNIEnv *env, jobject obj, jint cpu)
{
	struct cpufreq_policy *policy;
	jstring governorName;

	policy = cpufreq_get_policy((unsigned long)cpu);

	if (policy) {
		governorName = (*env)->NewStringUTF(env, policy->governor);
		//(*env)->DeleteLocalRef(env, governorName);
	}

	//cpufreq_put_policy(policy);

	return governorName;
}

JNIEXPORT jobjectArray JNICALL Java_platforms_x86_X86_1DVFS_getAvailableGovernors
  (JNIEnv *env, jobject obj, jint cpu)
{
	struct cpufreq_available_governors *govs, *first = NULL;
	jobjectArray govNames;
	jclass cls;
	jstring str = NULL;
	int i = 0, length = 0;

	govs = cpufreq_get_available_governors((unsigned int)cpu);

	if (govs)
		first = govs->first;

	while (govs) {
		govs = govs->next;
		length++;
	}

	cls = (*env)->FindClass(env, "Ljava/lang/String;");
	govNames = (*env)->NewObjectArray(env, (jsize) length, cls, NULL);

	if (!first)
		return govNames;

	govs = first;
	while (govs) {
		str = (*env)->NewStringUTF(env, govs->governor);
		(*env)->SetObjectArrayElement(env, govNames, i, str);
		(*env)->DeleteLocalRef(env, str);

		i++;
		govs = govs->next;
	}

	(*env)->DeleteLocalRef(env, cls);
	cpufreq_put_available_governors(first);
	return govNames;
}

JNIEXPORT jint JNICALL Java_platforms_x86_X86_1DVFS_setGovernor
  (JNIEnv *env, jobject obj, jint cpu, jstring governorName)
{
	const char *str;
	int ret;

	str = (*env)->GetStringUTFChars(env, governorName, NULL);

	if (str == NULL) {
		/* OutOfMemoryError already thrown */
		return -1;
	}

	ret = cpufreq_modify_policy_governor((unsigned int)cpu, (char *)str);

	(*env)->ReleaseStringUTFChars(env, governorName, str);

	return ret;
}

JNIEXPORT jlong JNICALL Java_platforms_x86_X86_1DVFS_getFrequency
  (JNIEnv *env, jobject obj, jint cpu)
{
	long ret = (long) cpufreq_get((unsigned int)cpu);
	return ret;
}

JNIEXPORT jlongArray JNICALL Java_platforms_x86_X86_1DVFS_getAvailableFrequencies
  (JNIEnv *env, jobject obj, jint cpu)
{
	struct cpufreq_available_frequencies *available_freqs, *first = NULL;
	jlongArray freqs;
	jlong c_freqs[16];
	int i = 0, length = 0;
	jclass cls;
	jfieldID fid;

	available_freqs = cpufreq_get_available_frequencies((unsigned int)cpu);

	if (available_freqs)
		first = available_freqs->first;

	/* First grab the number of available frequencies */
	while (available_freqs) {
		length++;
		available_freqs = available_freqs->next;
	}

	freqs = (*env)->NewLongArray(env, (jsize) length);

	if (!first) {
		cls = (*env)->GetObjectClass(env, obj);
		fid = (*env)->GetFieldID(env, cls, "minFreq", "J");
		(*env)->SetLongField(env, obj, fid, 1);
		(*env)->DeleteLocalRef(env, cls);
		return freqs;
	}

	available_freqs = first;

	while (available_freqs) {
		c_freqs[i] = (jlong)available_freqs->frequency;
		i++;
		available_freqs = available_freqs->next;
	}

	// move from the temp structure to the java structure
	(*env)->SetLongArrayRegion(env, freqs, 0, length, c_freqs);

	/* Free local references */
	cpufreq_put_available_frequencies(first);

	return freqs;
}

JNIEXPORT jint JNICALL Java_platforms_x86_X86_1DVFS_setFrequency
  (JNIEnv *env, jobject obj, jint cpu, jlong freq)
{
	int ret;

	ret = cpufreq_set_frequency((unsigned int)cpu, freq);

	return ret;
}

JNIEXPORT jint JNICALL Java_platforms_x86_X86_1DVFS_setFreqPolicy
  (JNIEnv *env, jobject obj, jint cpu, jlong minFreq, jlong maxFreq, jstring policyName)
{
	const char *str;
	int ret;

	str = (*env)->GetStringUTFChars(env, policyName, NULL);

	struct cpufreq_policy pol = { minFreq, maxFreq, (char *)str };

	ret = cpufreq_set_policy((unsigned int)cpu, &pol);

	(*env)->ReleaseStringUTFChars(env, policyName, str);

	return ret;
}

JNIEXPORT jobject JNICALL Java_platforms_x86_X86_1DVFS_getFreqStats
  (JNIEnv *env, jobject obj, jint cpu)
{
	jclass cls;
	jobject object;
	jmethodID mid;
	struct cpufreq_stats *stats, *first = NULL;
	unsigned long long total_time;

	/* Get FreqStat constructor and create object of said class */
	//cls = (*env)->FindClass(env, "FreqStats");

	cls = (*env)->FindClass(env, "utilities/dvfs/FreqStats");
	if (cls == NULL)
		return NULL;

	mid = (*env)->GetMethodID(env, cls, "<init>", "()V");

	if (mid == NULL)
		return NULL;

	object = (*env)->NewObject(env, cls, mid);

	stats = cpufreq_get_stats((unsigned int)cpu, &total_time);

	if (stats)
		first = stats->first;

	mid = (*env)->GetMethodID(env, cls, "addEntry", "(JJ)V");

	if (mid == NULL)
		return NULL;

	while (stats) {
		(*env)->CallObjectMethod(env, object, mid, (jlong) stats->frequency, (jlong) stats->time_in_state);
		stats = stats->next;
	}

	cpufreq_put_stats(first);

	/* Free local references */
	(*env)->DeleteLocalRef(env, cls);
	return object;
}
