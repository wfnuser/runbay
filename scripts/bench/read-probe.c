/* Same inner loop for native processes, Runbay sandboxes and the minimal OCI container. */
#include <fcntl.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

static long long nanos(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t)) return -1;
    return (long long)t.tv_sec * 1000000000 + t.tv_nsec;
}
int main(void) {
    long long entered = nanos();
    int fd = open("/dev/zero", O_RDONLY);
    if (fd < 0) return 1;
    char value;
    long long start = nanos();
    for (int i = 0; i < 100000; i++)
        if (read(fd, &value, 1) != 1) return 1;
    long long end = nanos();
    if (entered < 0 || start < entered || end < start) return 1;
    printf("%lld %lld 100000\n", entered, end - start);
    return 0;
}
