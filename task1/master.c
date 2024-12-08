#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <unistd.h>

enum {
    PORT_INDEX = 2,
    BUF_SIZE = 1024,
    MAX_WORKERS = 10,
};

void FindWorkers(int workers_count, int* workers, int* is_alive) {
    int sockfd;
    char buffer[BUF_SIZE];
    const char *hello = "master";
    struct sockaddr_in servaddr;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        printf("socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        printf("Error");
    }

    memset(&servaddr, 0, sizeof(servaddr));

    int n;
    socklen_t len = sizeof(servaddr);
    for (int i = 0; i < workers_count; ++i) {
        is_alive[i] = 0;

        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(workers[i]);
        servaddr.sin_addr.s_addr = INADDR_ANY;

        if (sendto(sockfd, (const char *)hello, strlen(hello), 0,
                   (const struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
            printf("error sendto %d", workers[i]);
            continue;
        }

        printf("Broadcast message sent to %d\n", workers[i]);

        n = recvfrom(sockfd, (char *)buffer, BUF_SIZE, 0,
                     (struct sockaddr *)&servaddr, &len);
        if (n == -1 && errno == EAGAIN) {
            printf("timeout for %d\n ", workers[i]);
            continue;
        }

        buffer[n] = '\0';
        printf("Master get: %s\n", buffer);
        if (strcmp(buffer, "worker") == 0) {
            is_alive[i] = 1;
        }
    }

    close(sockfd);
}

void Run(int workers_count, int* workers, int* is_alive) {
    int start, end;
    while (1) {
        scanf("%d %d", &start, &end);

        int socks[MAX_WORKERS] = {0};

        struct sockaddr_in servaddr;

        int ranges[MAX_WORKERS] = {0};

        int last = start;
        char buffer[BUF_SIZE];

        int sended = 0;
        while (sended != workers_count) {
            FindWorkers(workers_count, workers, is_alive);
            for (int i = 0; i < workers_count; ++i) {
                if (is_alive[i] == 0) {
                    continue;
                }
                socks[i] = socket(AF_INET, SOCK_STREAM, 0);
                if (socks[i] == -1) {
                    printf("socket creation failed...\n");
                    continue;
                }

                servaddr.sin_family = AF_INET;
                servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
                servaddr.sin_port = htons(workers[i]);

                if (connect(socks[i], (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0) {
                    is_alive[i] = 0;
                    printf("connection with the server failed...\n");
                    continue;
                }

                int n;
                if (i == workers_count - 1) {
                    ranges[i] = end;
                } else {
                    ranges[i] = (last + (rand() % (end + 1))) % (end + 1);
                }

                snprintf(buffer, BUF_SIZE, "%d %d %d", i, last, ranges[i]);

                last = ranges[i];

                write(socks[i], buffer, sizeof(buffer));

                ++sended;
            }
        }

        int get_ranges = 0;
        int is_ready[MAX_WORKERS] = {0};
        
        int range_id, range_res;

        int result = 0;
        while (1) {
            for (int i = 0; i < workers_count; ++i) {
                if (read(socks[i], buffer, sizeof(buffer)) != 0) {
                    sscanf(buffer, "%d %d", &range_id, &range_res);
                    printf("Get %d %d\n", range_id, range_res);
                    if (is_ready[range_id] == 0) {
                        is_ready[range_id] = 1;
                        result += range_res;
                        ++get_ranges;
                    }
                }
            }
            if (get_ranges == sended) {
                break;
            }
            FindWorkers(workers_count, workers, is_alive);
            for (int i = 0; i < workers_count; ++i) {
                if (is_alive[i] == 1) {
                    for (int j = 0; j < workers_count; ++j) {
                        if (is_ready[j] != 1) {
                            if (j == 0) {
                                snprintf(buffer, BUF_SIZE, "%d %d %d", j, 0, ranges[j]);
                            } else {
                                snprintf(buffer, BUF_SIZE, "%d %d %d", j, ranges[j - 1], ranges[j]);
                            }
                            write(socks[i], buffer, sizeof(buffer));
                        }
                    }
                }
            }

        }

        for (int i = 0; i < workers_count; ++i) {
            close(socks[i]);
        }

        printf("Result of integral: %d\n", result);
    }
}

int main(int argc, char *argv[]) {
    if (argc < PORT_INDEX) {
        printf("need port for start");
        return 1;
    }

    if (argc < PORT_INDEX + 1) {
        printf("need ports of workers for start");
        return 1;
    }

    int workers[MAX_WORKERS];

    printf("%d\n", argc);
    
    int workers_count = argc - 2;
    for (int i = 0; i < workers_count; ++i) {
        printf("%d\n", atoi(argv[i + 2]));
        workers[i] = atoi(argv[i + 2]);
    }

    int is_alive[MAX_WORKERS] = {0};

    int port = atoi(argv[1]);

    Run(workers_count, workers, is_alive);

    return 0;
}
