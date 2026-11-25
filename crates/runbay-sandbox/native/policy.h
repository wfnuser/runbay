/* Landlock's ABI grows monotonically. Handle all file mutations available through ABI 3,
 * including REFER and TRUNCATE when supported. Read-only mounts also cover ABI 1. */
#include <linux/audit.h>
#include <linux/capability.h>
#include <linux/filter.h>
#include <linux/landlock.h>
#include <linux/seccomp.h>
#include <stddef.h>
#include <sys/socket.h>

static const uint64_t read_rights = LANDLOCK_ACCESS_FS_READ_DIR | LANDLOCK_ACCESS_FS_READ_FILE |
                                    LANDLOCK_ACCESS_FS_EXECUTE;
#define FS_REFER (1ULL << 13)
#define FS_TRUNCATE (1ULL << 14)
static uint64_t handled_rights;

static int start_landlock(void) {
    int abi = syscall(SYS_landlock_create_ruleset, NULL, 0, LANDLOCK_CREATE_RULESET_VERSION);
    if (abi < 1) fail("query Landlock ABI");
    handled_rights = (1ULL << 13) - 1;
    if (abi >= 2) handled_rights |= FS_REFER;
    if (abi >= 3) handled_rights |= FS_TRUNCATE;
    struct landlock_ruleset_attr attr = { .handled_access_fs = handled_rights };
    int rules = syscall(SYS_landlock_create_ruleset, &attr, sizeof(attr.handled_access_fs), 0);
    if (rules < 0) fail("create Landlock ruleset");
    return rules;
}

static void allow_path(int rules, const char *name, uint64_t rights) {
    int fd = open(name, O_PATH | O_CLOEXEC);
    struct stat st;
    if (fd < 0 || fstat(fd, &st)) fail(name);
    if (!S_ISDIR(st.st_mode))
        rights &= LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_WRITE_FILE |
                  FS_TRUNCATE | LANDLOCK_ACCESS_FS_EXECUTE;
    struct landlock_path_beneath_attr path = { .allowed_access = rights, .parent_fd = fd };
    if (syscall(SYS_landlock_add_rule, rules, LANDLOCK_RULE_PATH_BENEATH, &path, 0))
        fail("add Landlock path rule");
    close(fd);
}

static void drop_caps(void) {
    for (int n = 0; ; n++) {
        if (prctl(PR_CAPBSET_DROP, n, 0, 0, 0) == 0) continue;
        if (errno == EINVAL) break;
        fail("drop capability bounding set");
    }
    struct __user_cap_header_struct header = { .version = _LINUX_CAPABILITY_VERSION_3 };
    struct __user_cap_data_struct empty[2] = {0};
    if (syscall(SYS_capset, &header, empty)) fail("drop effective capabilities");
}

#define LOAD(field) BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, field))
#define EQ(value, yes, no) BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, (value), (yes), (no))
#define RET(value) BPF_STMT(BPF_RET | BPF_K, (value))
#define DENY(nr) EQ(SYS_ ## nr, 0, 1), RET(SECCOMP_RET_ERRNO | EPERM)

static void install_filter(int network) {
#if defined(__aarch64__)
    const unsigned int arch = AUDIT_ARCH_AARCH64;
#elif defined(__x86_64__)
    const unsigned int arch = AUDIT_ARCH_X86_64;
#else
#error "Runbay native sandbox requires an audited syscall architecture"
#endif
    struct sock_filter code[] = {
        LOAD(arch), EQ(arch, 1, 0), RET(SECCOMP_RET_KILL_PROCESS), LOAD(nr),
#ifdef __x86_64__
        BPF_JUMP(BPF_JMP | BPF_JGE | BPF_K, 0x40000000, 0, 1), RET(SECCOMP_RET_KILL_PROCESS),
#endif
        DENY(setns), DENY(unshare), DENY(mount), DENY(umount2), DENY(pivot_root),
        DENY(open_tree), DENY(move_mount), DENY(mount_setattr), DENY(fsopen), DENY(fsmount),
        DENY(fsconfig), DENY(fspick), DENY(chroot), DENY(ptrace), DENY(process_vm_readv),
        DENY(process_vm_writev), DENY(pidfd_getfd), DENY(open_by_handle_at), DENY(bpf),
        DENY(perf_event_open), DENY(userfaultfd), DENY(io_uring_setup), DENY(keyctl),
        DENY(add_key), DENY(request_key), DENY(mknodat), DENY(reboot),
#ifdef SYS_mknod
        DENY(mknod),
#endif
        /* clone3's flags live behind a pointer. ENOSYS enables libc's ordinary clone path. */
        EQ(SYS_clone3, 0, 1), RET(SECCOMP_RET_ERRNO | ENOSYS),
        EQ(SYS_clone, 0, 3), LOAD(args[0]),
        BPF_JUMP(BPF_JMP | BPF_JSET | BPF_K,
                 CLONE_NEWUSER | CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWNET |
                 CLONE_NEWIPC | CLONE_NEWUTS | CLONE_NEWCGROUP, 0, 1),
        RET(SECCOMP_RET_ERRNO | EPERM), LOAD(nr),
        EQ(SYS_socket, 1, 0), EQ(SYS_socketpair, 0, 3), LOAD(args[0]),
        EQ(AF_UNIX, 1, 0), RET(network ? SECCOMP_RET_ALLOW : SECCOMP_RET_ERRNO | EAFNOSUPPORT),
        RET(SECCOMP_RET_ALLOW),
    };
    struct sock_fprog prog = { .len = sizeof(code)/sizeof(code[0]), .filter = code };
    if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog)) fail("install seccomp filter");
}
