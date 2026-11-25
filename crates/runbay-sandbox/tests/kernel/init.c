/* Minimal offline init and worker for the 5.13 compatibility test. No shell or bwrap in the image. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/landlock.h>
#include <linux/seccomp.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/reboot.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

static void check(int ok, const char *name) {
    if (ok) return;
    dprintf(2, "RUNBAY FAIL %s: %s\n", name, strerror(errno));
    if (getpid() == 1 && getuid() == 0) reboot(RB_POWER_OFF);
    _exit(1);
}
static void dir(const char *path) { check(mkdir(path, 0755) == 0 || errno == EEXIST, path); }
static void copy(const char *from, const char *to) {
    int in = open(from, O_RDONLY);
    int out = open(to, O_CREAT | O_WRONLY, 0755);
    check(in >= 0 && out >= 0, "copy open");
    ssize_t n;
    do { n = sendfile(out, in, NULL, 1024 * 1024); } while (n > 0);
    check(n == 0, "copy");
    close(in); close(out);
}
static ino_t netns(void) {
    struct stat st;
    check(stat("/proc/self/ns/net", &st) == 0, "network namespace inode");
    return st.st_ino;
}
static const struct sockaddr_un address = { .sun_family = AF_UNIX, .sun_path = "/gateway.sock" };
static void worker(const char *outside) {
    check(getuid() == 1000 && getpid() == 1, "unprivileged namespace init");
    check(netns() != strtoull(outside, NULL, 10), "independent network namespace");
    check(prctl(PR_GET_SECCOMP) == SECCOMP_MODE_FILTER && prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) == 1,
          "seccomp and no_new_privs");
    check(open("/ws/answer", O_WRONLY | O_CREAT | O_EXCL, 0600) >= 0, "workspace write");
    struct statvfs fs;
    struct stat tmp;
    check(statvfs("/tmp", &fs) == 0 && !(fs.f_flag & ST_RDONLY) &&
          stat("/tmp", &tmp) == 0 && (tmp.st_mode & 0777) == 0777, "writable test mount");
    check(open("/tmp/new-file", O_WRONLY | O_CREAT, 0600) < 0 && errno == EACCES, "Landlock write denial");
    check(open("/sibling", O_RDONLY) < 0 && errno == ENOENT, "private file hidden");
    check(truncate("/readonly", 0) < 0 && (errno == EROFS || errno == EACCES), "readonly truncate");
    check(socket(AF_INET, SOCK_STREAM, 0) < 0 && errno == EAFNOSUPPORT, "internet denied");
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    check(sock >= 0 && connect(sock, (void *)&address, sizeof(address)) == 0, "UDS connect");
    char reply;
    check(write(sock, "Q", 1) == 1 && read(sock, &reply, 1) == 1 && reply == 'A', "UDS exchange");
}
int main(int argc, char **argv) {
    if (argc == 3 && !strcmp(argv[1], "worker")) { worker(argv[2]); return 0; }
    if (argc == 1) {
        /* pivot_root cannot detach the kernel's initial rootfs. Boot into a normal tmpfs root. */
        dir("/root");
        check(mount("tmpfs", "/root", "tmpfs", 0, NULL) == 0, "boot root");
        copy("/init", "/root/init"); copy("/runbay-landlock", "/root/runbay-landlock");
        check(chdir("/root") == 0 && mount(".", "/", NULL, MS_MOVE, NULL) == 0 &&
              chroot(".") == 0 && chdir("/") == 0, "switch root");
        execl("/init", "/init", "test", NULL);
        check(0, "start test");
    }
    dir("/proc"); dir("/tmp"); dir("/ws");
    check(mount("proc", "/proc", "proc", 0, NULL) == 0, "host proc");
    check(chown("/ws", 1000, 1000) == 0, "own workspace");
    int file = open("/readonly", O_CREAT | O_RDWR, 0666);
    check(file >= 0 && write(file, "sample", 6) == 6 && fchmod(file, 0666) == 0, "create public input");
    close(file);
    check(close(open("/sibling", O_CREAT | O_WRONLY, 0644)) == 0, "create sibling");
    int server = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    check(server >= 0 && bind(server, (void *)&address, sizeof(address)) == 0 &&
          chmod(address.sun_path, 0777) == 0 && listen(server, 1) == 0, "host UDS");
    struct utsname k;
    uname(&k);
    int abi = syscall(SYS_landlock_create_ruleset, NULL, 0, LANDLOCK_CREATE_RULESET_VERSION);
    dprintf(1, "RUNBAY kernel=%s arch=%s Landlock_ABI=%d\n", k.release, k.machine, abi);
    check(!strncmp(k.release, "5.13.", 5) && abi >= 1, "Linux 5.13 with Landlock");
    char outside[32];
    snprintf(outside, sizeof(outside), "%lu", (unsigned long)netns());
    pid_t child = fork();
    check(child >= 0, "fork launcher");
    if (!child) {
        check(setgid(1000) == 0 && setuid(1000) == 0, "switch to ordinary user");
        execl("/runbay-landlock", "runbay-landlock", "--workspace", "/ws", "--ro", "/init",
              "--ro", "/gateway.sock", "--ro", "/readonly", "--", "/init", "worker", outside, NULL);
        check(0, "launch");
    }
    struct pollfd p = { .fd = server, .events = POLLIN };
    check(poll(&p, 1, 10000) == 1, "wait for worker UDS");
    int connection = accept(server, NULL, NULL);
    char question;
    check(connection >= 0 && read(connection, &question, 1) == 1 && question == 'Q' &&
          write(connection, "A", 1) == 1, "host exchange");
    int status;
    check(waitpid(child, &status, 0) == child && WIFEXITED(status) && !WEXITSTATUS(status), "worker exit");
    struct stat st;
    check(stat("/readonly", &st) == 0 && st.st_size == 6 && access("/ws/answer", F_OK) == 0, "host output");
    dprintf(1, "RUNBAY PASS Linux 5.13 native sandbox and UDS\n");
    reboot(RB_POWER_OFF);
    return 0;
}
