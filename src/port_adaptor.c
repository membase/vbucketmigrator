/*
 * Wrapper program to make general applications into erlang ports.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sysexits.h>
#include <sys/wait.h>
#include <setjmp.h>
#include <errno.h>

#include <assert.h>

static sig_atomic_t caught;
static int grace;

static void caught_signal(int which)
{
    caught = which;
    /* just close stdin as we want fgets to return */
    int saved_errno = errno;
    close(0);
    errno = saved_errno;
}

static int wait_for_process(pid_t pid)
{
    int rv = EX_SOFTWARE;
    int stats = 0;
    int i = 0;
    pid_t p;

    int ch;
    do {
        ch = getc(stdin);
    } while (ch != EOF && ch != '\n');

    if (caught != SIGCHLD) {
        if (kill(pid, SIGTERM) < 0) {
            perror("Failed to kill my child.");
            exit(EX_SOFTWARE);
        }
    }

    /* Loop forever waiting for the process to quit */
    for (i = 0; ;i++) {
        alarm(grace);
        p = waitpid(pid, &stats, 0);
        /* make sure we know if child is dead */
	while (p == (pid_t)-1 && errno == EINTR) {
		p = waitpid(pid, &stats, WNOHANG);
	}
        if (p == pid) {
            /* child exited.  Let's get out of here */
            rv = WIFEXITED(stats) ?
                WEXITSTATUS(stats) :
                (0x80 | WTERMSIG(stats));
            break;
        } else {
           printf("Undead, sent sig N9NE\n");
           if (kill(pid, SIGKILL) < 0) {
               /* Kill failed.  Must have lost the process. :/ */
               perror("lost child when trying to kill");
               exit(EX_SOFTWARE);
           }
        }
    }

    return rv;
}

static int spawn_and_wait(int argc, char **argv)
{
    int rv = EX_SOFTWARE;
    pid_t pid = fork();

    assert(argc > 0);

    switch (pid) {
    case -1:
        perror("fork");
        rv = EX_OSERR;
        break; /* NOTREACHED */
    case 0:
        execvp(argv[0], argv);
        perror("exec");
        rv = EX_SOFTWARE;
        break; /* NOTREACHED */
    default:
        printf("%d\n", pid);
        fflush(stdout);
        rv = wait_for_process(pid);
    }
    return rv;
}

int main(int argc, char **argv)
{
    assert(argc > 2);
    grace = atoi(argv[1]);

    struct sigaction sig_handler;

    sig_handler.sa_handler = caught_signal;
    sig_handler.sa_flags = 0;

    sigaction(SIGALRM, &sig_handler, NULL);
    sigaction(SIGHUP, &sig_handler, NULL);
    sigaction(SIGINT, &sig_handler, NULL);
    sigaction(SIGTERM, &sig_handler, NULL);
    sigaction(SIGPIPE, &sig_handler, NULL);
    sigaction(SIGCHLD, &sig_handler, NULL);

    return spawn_and_wait(argc-2, argv+2);
}
