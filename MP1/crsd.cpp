#include <glog/logging.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "interface.h"

const int PORT = 8080;

int main(int argc, char *argv[]){
    // Change log location to a dedicated folder
    FLAGS_log_dir = "./logs/";
    // Also log to the terminal
    FLAGS_alsologtostderr = 1;
    google::InitGoogleLogging(argv[0]);

    LOG(INFO) << "Starting Server";

    // initialize control socket
    struct sockaddr_in control_addr;
    memset(&control_addr, 0, sizeof(control_addr));

    // create socket
    int control_fd;
    if ((control_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG(ERROR) << "ERROR: could not open socket";
        exit(EXIT_FAILURE);
    }

    control_addr.sin_family = AF_INET;
    control_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    control_addr.sin_port = htons(PORT);

    // bind the socket
    if (bind(control_fd, (struct sockaddr*) &control_addr, sizeof(control_addr)) < 0) {
        LOG(ERROR) << "ERROR: could not bind socket";
        exit(EXIT_FAILURE);
    }

    // listen socket
    if (listen(control_fd, 5) < 0) {
        LOG(ERROR) << "ERROR: could not listen on socket";
        exit(EXIT_FAILURE);
    }

    LOG(INFO) << "Server ready for connections";

    while (true) {
        // accept a client
        int client_fd;
        struct sockaddr_in client_addr;
        int client_size = sizeof(struct sockaddr_in);

        if (client_fd = accept(control_fd, (struct sockaddr*) &client_addr, (socklen_t*) &client_size) < 0) {
            LOG(ERROR) << "ERROR: accept failed";
            continue;
        }

        LOG(INFO) << "Client accepted";
    }
}

