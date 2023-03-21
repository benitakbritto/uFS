
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "fsapi.h"
#include "testClient_common.h"
#include "util.h"

//
// A simple CLI to test the functionality of file system
//

bool interactive = true;

#ifdef TEST_VFS_INSTEAD
#define fs_read read
#define fs_pread pread
#define fs_cached_posix_pread pread
#define fs_write write
#define fs_pwrite pwrite
#define fs_readdir readdir
#define fs_fstat fstat
#define fs_close close
#define fs_fdatasync fdatasync
#define fs_syncall sync
#define fs_syncunlinked sync
#define fs_ping sync
#define fs_dumpinodes
#define fs_migrate
#define fs_start_dump_load_stats
#define fs_stop_dump_load_stats
void clean_exit() { exit(0); };
int cur_dir_fd = 0;
#else
void clean_exit() {
  int ret = fs_exit();
  if (ret < 0) exit(ret);
  exit(fs_cleanup());
};
#endif

void handle_sigint(int sig);

void printHelp() {
  printf(ANSI_COLOR_CYAN "Help:" ANSI_COLOR_RESET "\n");
  printf(ANSI_COLOR_GREEN "cp -r" ANSI_COLOR_RESET " <PATH & FILENAME SRC> <PATH & FILENAME SRC>\n");
}

void printReturnValue(std::string const &cmd, ssize_t v) {
  if (interactive) {
    printf(ANSI_COLOR_MAGENTA "%s Return:" ANSI_COLOR_RESET "\n", cmd.c_str());
    printf(ANSI_COLOR_BLUE "%ld" ANSI_COLOR_RESET "\n", v);
    printf(ANSI_COLOR_MAGENTA "-------" ANSI_COLOR_RESET "\n");
  } else {
    printf("return %ld\n", v);
    fflush(stdout);
  }
}

static void randStr(char *dest, size_t length) {
  char charset[] =
      "0123456789"
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  while (length-- > 0) {
    size_t index = (double)rand() / RAND_MAX * (sizeof charset - 1);
    *dest++ = charset[index];
  }
  *dest = '\0';
}

static uint64_t write_char_no = 0;

void process(std::string const &line) {
  if (line == "help") {
    printHelp();
    return;
  } else if (line == "quit" || line == "exit") {
    clean_exit();
  }

  std::istringstream iss(line);
  std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                  std::istream_iterator<std::string>{}};
  if (!tokens.empty()) {
    if (tokens[0] == "cp") {
      if (tokens.size() != 4) {
        // TODO
        int ret = fs_cp(tokens[2].c_str(), tokens[3].c_str());
        
        printReturnValue(tokens[0].c_str(), ret);
      } else {
        printHelp();
      }
    } else {
      printHelp();
    }
  }
}

void cliMain() {
  for (std::string line; std::cout << (interactive ? "FsApp > " : "") &&
                         std::getline(std::cin, line);) {
    if (!line.empty()) {
      process(line);
    }
  }
}

int main(int argc, char **argv) {
#ifndef TEST_VFS_INSTEAD
  if (argc != 2) {
    fprintf(stderr, "Usage %s <pid(from 0)>|<pid1,pid2,pid3>\n", argv[0]);
    fprintf(stderr, "\t requires only one argument\n");
    fprintf(stderr, "\t Option-1 --- pid: one integer as user-supply PID\n");
    fprintf(stderr,
            "\t Option-2 --- pid1,pid2,pid3: a list of integers separated by , "
            "(comma)\n");
    exit(1);
  }
#else
  if (argc != 2) {
    fprintf(stderr, "Usage %s [base path]\n", argv[0]);
    fprintf(stderr, "[base path] should be a writable directory\n");
    exit(1);
  }
#endif
  interactive = isatty(STDIN_FILENO);
  if (!interactive && (errno != ENOTTY)) {
    fprintf(stderr, "Cannot access standard input\n");
    return -1;
  }
  if (!interactive) {
    printf("Running in batch mode\n");
  }
#ifndef TEST_VFS_INSTEAD
  // int rt = fs_init(FS_SHM_KEY_BASE + stoiWrapper(argv[1]));
  const int kMaxNumFsp = 10;
  key_t shmKeys[kMaxNumFsp];
  for (int i = 0; i < kMaxNumFsp; i++) shmKeys[i] = 0;
  auto pidStrVec = splitStr(argv[1], ',');
  int numKey = pidStrVec.size();
  assert(numKey <= kMaxNumFsp);
  for (int i = 0; i < numKey; i++) {
    shmKeys[i] = (FS_SHM_KEY_BASE) + stoiWrapper(pidStrVec[i]);
  }
#ifdef UFS_SOCK_LISTEN
  int rg_rt = fs_register();
  fprintf(stdout, "register rt:%d\n", rg_rt);
#else
  int rg_rt = -1;
#endif
  if (rg_rt < 0) {
    int rt = fs_init_multi(numKey, shmKeys);
    if (rt < 0) {
      return -1;
    }
  }
#else
  cur_dir_fd = open(argv[1], O_DIRECTORY | O_RDONLY);
  if (cur_dir_fd == -1) {
    perror("Failed to open directory");
    return -1;
  }
#endif
  signal(SIGINT, handle_sigint);
  cliMain();
  clean_exit();
  return 0;
}

void handle_sigint(int sig) {
  std::cout << std::endl << "Bye :)" << std::endl;
  clean_exit();
}
