#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>
#include <unistd.h>

enum {
    PORT_INDEX = 2,
    BUF_SIZE = 1024,
};

int port;

void* FindMaster() {
    int sockfd;
    char buffer[BUF_SIZE];

    struct sockaddr_in servaddr, cliaddr;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        printf("socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    memset(&cliaddr, 0, sizeof(cliaddr));

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(port);

    if (bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr)) <
        0) {
        printf("bind failed");
        exit(EXIT_FAILURE);
    }

    socklen_t len;
    int n;

    len = sizeof(cliaddr);

    const char *msg = "worker";
    for (;;) {
        n = recvfrom(sockfd, (char *)buffer, BUF_SIZE, 0,
                     (struct sockaddr *)&cliaddr, &len);
        if (n > 0) {
            buffer[n] = '\0';
            printf("Worker get: %s\n", buffer);
            fflush(stdout);
            if (strcmp(buffer, "master") == 0) {
                if (sendto(sockfd, msg, strlen(msg), 0,
                           (struct sockaddr *)&cliaddr, len) < 0) {
                    printf("error sendto");
                }
            }
        }
    }

    close(sockfd);
}

// f(x) = x
int HardWork(int a, int b) {
    return b - a;
}

void Work() {
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
        exit(EXIT_FAILURE);
    } else {
        printf("Socket successfully created..\n");
    }
    bzero(&servaddr, sizeof(servaddr));

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if ((bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr))) != 0) {
        printf("socket bind failed...\n");
        exit(EXIT_FAILURE);
    }

    if ((listen(sockfd, 5)) != 0) {
        printf("Listen failed...\n");
        exit(EXIT_FAILURE);
    }

    socklen_t len = sizeof(cli);

    char buffer[BUF_SIZE];
    int n;
    for (;;) {
        connfd = accept(sockfd, (struct sockaddr *)&cli, &len);
        if (connfd < 0) {
            printf("server accept failed...\n");
            continue;
        }

        n = read(connfd, buffer, sizeof(buffer));
        if (n < 0) {
            close(connfd);
            continue;
        }

        buffer[n] = '\0';
        int range_id, a, b;

        sscanf(buffer, "%d %d %d", &range_id, &a, &b);

        int res = HardWork(a, b);

        printf("Hard work res: %d\n", res);

        snprintf(buffer, BUF_SIZE, "%d %d\n", range_id, res);

        while (write(connfd, buffer, strlen(buffer)) < 0) {
        }
    }

    close(sockfd);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("need port for start");
        return 1;
    }

    port = atoi(argv[1]);

    pthread_t live_thread;
    pthread_create(&live_thread, NULL, FindMaster, NULL);

    Work();

    return 0;
}
