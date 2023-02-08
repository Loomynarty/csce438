#include <glog/logging.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <pthread.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "interface.h"

int num_clients = 0;

struct client_info {
    int fd;
    int port;
    struct sockaddr_in addr;
};

Reply handle_create(char* buffer, int fd) {
    LOG(INFO) << "Create command received";

    // Create reply and send
    Reply reply;
    reply.status = SUCCESS;

    return reply;
}

Reply handle_delete(char* buffer, int fd) {
    LOG(INFO) << "Delete command received";
    // Create reply and send
    Reply reply;
    reply.status = SUCCESS;
    
    return reply;
}

Reply handle_join(char* buffer, int fd) {
    LOG(INFO) << "Join command received";
    // Create reply and send
    Reply reply;
    reply.status = SUCCESS;
    
    return reply;
}


Reply handle_list(char* buffer, int fd) {
    LOG(INFO) << "List command received";
    // Create reply and send
    Reply reply;
    reply.status = SUCCESS;
    
    return reply;
}

void parse_command(char* buffer, int fd) {
    Reply reply;
    char resp[MAX_DATA];

    LOG(INFO) << "parse_command buffer: " << buffer;

    if (strncmp(buffer, "CREATE", 6) == 0){
        reply = handle_create(buffer, fd);
    } 
    else if (strncmp(buffer, "DELETE", 6) == 0){
        reply = handle_delete(buffer, fd);
    } 
    else if (strncmp(buffer, "JOIN", 4) == 0){
        reply = handle_join(buffer, fd);
    } 
    else if (strncmp(buffer, "LIST", 4) == 0){
        reply = handle_list(buffer, fd);
    }
    else {
        reply.status = FAILURE_INVALID;
    }

    // copy reply into response
    memcpy(resp, (void*) &reply, sizeof(reply));
    // send the response
    if (send(fd, resp, MAX_DATA, 0) < 0)
    {
        LOG(ERROR) << "ERROR: send failed";
        exit(EXIT_FAILURE);		
    }
}

void* handle_connection(void* fd) {
    int bytes;
    char buffer[MAX_DATA];
    int client_fd = *(int*) fd;

    // Receive commands from client
    while (true) {
        bytes = (recv(client_fd, buffer, MAX_DATA, 0));
        
        if (bytes <= 0) {
            //LOG(INFO) << "Client " << client_fd << " aborted";
            break;
        }

        // Parse command
        parse_command(buffer, client_fd);

    }

    // broke out of loop -- close the client socket
    LOG(INFO) << "Client " << client_fd << " connection terminated";
    close(client_fd);
    num_clients--;
    return NULL;
}

int main(int argc, char *argv[]) {
    // Default port
    int port = 8080;
    if (argc > 1) {
        port = atoi(argv[1]);
    }

    // Change log location to a dedicated folder
    FLAGS_log_dir = "../logs/";
    // Also log to the terminal
    FLAGS_alsologtostderr = 1;
    google::InitGoogleLogging(argv[0]);

    LOG(INFO) << "Starting server on port " << port;

    // initialize control socket
    struct sockaddr_in control_addr;
    memset(&control_addr, 0, sizeof(control_addr));

    // create socket
    int control_fd;
    if ((control_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG(ERROR) << "ERROR: could not open socket";
        exit(EXIT_FAILURE);
    }

    const int enable = 1;
    // set socket to allow reuse
    if (setsockopt(control_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &enable, sizeof(int)) < 0) {
        LOG(ERROR) << "ERROR: setsockopt failed";
        exit(EXIT_FAILURE);
    }

    control_addr.sin_family = AF_INET;
    control_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    control_addr.sin_port = htons(port);

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

        if ((client_fd = accept(control_fd, (struct sockaddr*) &client_addr, (socklen_t*) &client_size)) < 0) {
            LOG(ERROR) << "ERROR: accept failed";
            continue;
        }

        LOG(INFO) << "Client accepted: " << client_fd;
        num_clients++;
        LOG(INFO) << "Current number of clients: " << num_clients;

        pthread_t handler_thread;
        pthread_create(&handler_thread, NULL, &handle_connection, &client_fd);
    }
    close(control_fd);
}

