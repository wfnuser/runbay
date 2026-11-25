/* Static, unprivileged Linux launcher. Only this single-threaded process sets up the worker.
 * The parent stays outside the PID namespace to propagate the worker's exit status. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/mount.h>
#include <linux/openat2.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

/* Linux UAPI since 5.11. Older libc headers may not expose the flag. */
#ifndef CLOSE_RANGE_CLOEXEC
#define CLOSE_RANGE_CLOEXEC (1U << 2)
#endif

static int ready = -1;

static void fail(const char *stage) {
    int error = errno;
    if (ready >= 0) dprintf(ready, "%s: %s", stage, strerror(error));
    dprintf(STDERR_FILENO, "runbay-landlock: %s: %s\n", stage, strerror(error));
    _exit(125);
}
#include "policy.h"

static void write_map(const char *path, const char *value) {
    int fd = open(path, O_WRONLY | O_CLOEXEC);
    ssize_t length = (ssize_t)strlen(value);
    if (fd < 0 || write(fd, value, length) != length) fail(path);
    close(fd);
}

static void namespaces(int network) {
    uid_t uid = getuid();
    gid_t gid = getgid();
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) fail("no new privileges");
    if (unshare(CLONE_NEWUSER)) fail("create user namespace");
    char mapping[64];
    snprintf(mapping, sizeof(mapping), "%u %u 1\n", uid, uid);
    write_map("/proc/self/uid_map", mapping);
    write_map("/proc/self/setgroups", "deny\n");
    snprintf(mapping, sizeof(mapping), "%u %u 1\n", gid, gid);
    write_map("/proc/self/gid_map", mapping);
    if (prctl(PR_SET_DUMPABLE, 0)) fail("protect launcher");
    int namespaces = CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWIPC | CLONE_NEWUTS;
    if (unshare(namespaces | (network ? 0 : CLONE_NEWNET))) fail("create worker namespaces");
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL)) fail("private mount propagation");
}

struct exposure {
    const char *path;
    int tree, writable, device, dir;
};

static int safe_absolute(const char *path) {
    if (path[0] != '/' || path[1] == '\0' || path[strlen(path)-1] == '/') return 0;
    for (const char *p = path + 1; *p; ) {
        size_t n = strcspn(p, "/");
        if (!n || (n == 1 && *p == '.') || (n == 2 && !strncmp(p, "..", 2))) return 0;
        p += n;
        if (*p) p++;
    }
    return 1;
}

/* Pin the source before hiding the host's /tmp. Clone a single mount, without importing
 * nested host filesystems. The destination will be an empty placeholder on our own tmpfs. */
static void pin(struct exposure *e) {
    char resolved[PATH_MAX];
    const char *source = e->path;
    /* Distribution-owned compatibility symlinks are allowed. Run-specific paths were
     * resolved by the daemon and must never follow a replacement symlink at launch. */
    if (!strcmp(source, "/bin") || !strcmp(source, "/sbin") || !strcmp(source, "/lib")
        || !strcmp(source, "/lib64") || !strcmp(source, "/etc/resolv.conf")
        || !strcmp(source, "/etc/ssl/certs")) {
        if (!realpath(source, resolved)) fail(source);
        source = resolved;
    }
    struct open_how how = { .flags = O_PATH | O_CLOEXEC,
                            .resolve = RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS };
    int fd = syscall(SYS_openat2, AT_FDCWD, source, &how, sizeof(how));
    struct stat st;
    if (fd < 0 || fstat(fd, &st)) fail(e->path);
    e->dir = S_ISDIR(st.st_mode);
    e->tree = syscall(SYS_open_tree, fd, "", AT_EMPTY_PATH | OPEN_TREE_CLONE | OPEN_TREE_CLOEXEC);
    if (e->tree < 0) fail("clone source mount");
    close(fd);
    struct mount_attr attr = {
        .attr_set = MOUNT_ATTR_NOSUID | (e->device ? 0 : MOUNT_ATTR_NODEV)
                    | (e->writable ? 0 : MOUNT_ATTR_RDONLY),
    };
    if (syscall(SYS_mount_setattr, e->tree, "", AT_EMPTY_PATH, &attr, sizeof(attr)))
        fail("restrict source mount");
}

static void make_dir(const char *path) {
    if (mkdir(path, 0755) && errno != EEXIST) fail(path);
}

static void placeholder(const struct exposure *e) {
    char path[PATH_MAX];
    if (strlen(e->path) >= sizeof(path)) { errno = ENAMETOOLONG; fail("exposure"); }
    strcpy(path, e->path + 1);
    for (char *p = path; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        make_dir(path);
        *p = '/';
    }
    if (e->dir) {
        make_dir(path);
    } else {
        int fd = open(path, O_CREAT | O_EXCL | O_CLOEXEC | O_WRONLY, 0600);
        if (fd < 0) fail(path);
        close(fd);
    }
}

static void filesystem(struct exposure *items, int count) {
    for (int i = 0; i < count; i++) pin(&items[i]);
    if (mount("tmpfs", "/tmp", "tmpfs", MS_NOSUID | MS_NODEV, "mode=0755,size=4m") ||
        chdir("/tmp")) fail("create private root");
    make_dir("tmp");
    make_dir("proc");
    /* An empty, private /tmp holds run-specific mount points. Landlock grants writes
     * only on explicit writable exposures below it. */
    if (mount("tmpfs", "tmp", "tmpfs", MS_NOSUID | MS_NODEV | MS_NOEXEC, "mode=1777,size=4m"))
        fail("mount private temporary directory");
    for (int i = 0; i < count; i++) placeholder(&items[i]);
    /* Pin every destination before attaching any sources. No pathname lookup can follow a
     * worker-controlled symlink into another mount, including during concurrent renames. */
    int *targets = calloc(count, sizeof(int));
    if (!targets) fail("allocate destinations");
    for (int i = 0; i < count; i++) {
        targets[i] = open(items[i].path + 1, O_PATH | O_NOFOLLOW | O_CLOEXEC);
        if (targets[i] < 0) fail("open destination");
    }
    for (int i = 0; i < count; i++) {
        if (syscall(SYS_move_mount, items[i].tree, "", targets[i], "",
                    MOVE_MOUNT_F_EMPTY_PATH | MOVE_MOUNT_T_EMPTY_PATH)) fail("attach private mount");
        close(items[i].tree);
        close(targets[i]);
    }
    free(targets);
    if (mount("proc", "proc", "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC | MS_RDONLY, NULL))
        fail("mount private proc");
    if (syscall(SYS_pivot_root, ".", ".") || umount2(".", MNT_DETACH) || chdir("/"))
        fail("detach original root");
    struct mount_attr attr = { .attr_set = MOUNT_ATTR_RDONLY };
    if (syscall(SYS_mount_setattr, AT_FDCWD, "/", 0, &attr, sizeof(attr))) fail("seal private root");
}

/* Executed once per run. The handshake descriptor closes on successful exec, and reports
 * any setup/exec error to the daemon before it marks the run as started. */
int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "--print-policy-id")) {
        puts(RUNBAY_POLICY_ID);
        return 0;
    }
    char *worker_argv0 = NULL;
    struct exposure *items = calloc(argc, sizeof(*items));
    if (!items) fail("allocate exposures");
    const char *workspace = NULL;
    int network = 0, n = 0, i = 1;
    for (; i < argc && strcmp(argv[i], "--"); i++) {
        if (!strcmp(argv[i], "--network")) { network = 1; continue; }
        const char *option = argv[i];
        if (++i >= argc) { errno = EINVAL; fail("missing option argument"); }
        if (!strcmp(option, "--ready")) {
            ready = atoi(argv[i]);
            if (ready < 3 || fcntl(ready, F_SETFD, FD_CLOEXEC)) fail("setup handshake");
            continue;
        }
        if (!strcmp(option, "--expect-policy")) {
            if (strcmp(argv[i], RUNBAY_POLICY_ID)) { errno = EPROTO; fail("launcher policy mismatch"); }
            continue;
        }
        if (!strcmp(option, "--argv0")) { worker_argv0 = argv[i]; continue; }
        int ws = !strcmp(option, "--workspace"), rw = !strcmp(option, "--rw");
        int dev = !strcmp(option, "--device");
        if ((!ws && !rw && !dev && strcmp(option, "--ro")) || !safe_absolute(argv[i])) {
            errno = EINVAL; fail("invalid exposure");
        }
        if (ws) workspace = argv[i];
        items[n++] = (struct exposure){ .path = argv[i], .writable = ws || rw ||
            (dev && !strcmp(argv[i], "/dev/null")), .device = dev };
    }
    if (!workspace || ++i >= argc || !safe_absolute(argv[i])) { errno = EINVAL; fail("worker arguments"); }
    int parent = syscall(SYS_pidfd_open, getppid(), 0);
    if (parent < 0 || prctl(PR_SET_PDEATHSIG, SIGKILL)) fail("watch daemon");
    namespaces(network);
    int monitor = syscall(SYS_pidfd_open, getpid(), 0);
    if (monitor < 0) fail("watch sandbox monitor");
    pid_t child = fork();
    if (child < 0) fail("create PID namespace init");
    if (child) {
        if (ready >= 0) close(ready);
        close(monitor);
        struct pollfd p = { .fd = parent, .events = POLLIN };
        if (poll(&p, 1, 0) != 0) _exit(125);
        close(parent);
        int status;
        while (waitpid(child, &status, 0) < 0) if (errno != EINTR) _exit(125);
        return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    }
    close(parent);
    struct pollfd p = { .fd = monitor, .events = POLLIN };
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) || poll(&p, 1, 0) != 0) fail("monitor exited");
    close(monitor);
    filesystem(items, n);
    if (chdir(workspace)) fail("enter workspace");
    int rules = start_landlock();
    allow_path(rules, "/", read_rights);
    for (int j = 0; j < n; j++)
        if (items[j].writable) allow_path(rules, items[j].path, handled_rights);
    if (syscall(SYS_landlock_restrict_self, rules, 0)) fail("enforce Landlock");
    close(rules);
    drop_caps();
    install_filter(network);
    /* Prevent unrelated inherited descriptors from carrying host authority across exec. */
    if (syscall(SYS_close_range, 3U, ~0U, CLOSE_RANGE_CLOEXEC)) fail("seal file descriptors");
    if (ready >= 0 && write(ready, "R", 1) != 1) fail("notify daemon");
    const char *program = argv[i];
    if (worker_argv0) argv[i] = worker_argv0;
    execv(program, &argv[i]);
    fail(program);
}
