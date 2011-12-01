CC=gcc
LIBNAME=x86DVFS.so

dvfs: dvfs.o
	$(CC) -shared -Wl,-soname,$(LIBNAME) \
	-static-libgcc -mimpure-text -L. -o lib$(LIBNAME) \
	dvfs.o -lc -lcpufreq

dvfs.o: dvfs.c
	$(CC) -O2 -fPIC -fno-strict-aliasing \
	-W -Wall -Wno-parentheses \
	-DLINUX -I/usr/lib/jvm/default-java/include/ \
	-I/usr/lib/jvm/default-java/include/linux -c dvfs.c
	
clean:
	rm -f *.o lib$(LIBNAME) *.class
