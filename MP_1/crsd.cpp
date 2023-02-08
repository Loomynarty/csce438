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

// Default port
int port = 8080;
int num_clients = 0;
std::vector<Room*> room_db;
pthread_mutex_t room_mtx = PTHREAD_MUTEX_INITIALIZER;

void room_message(Room* room, char* message, int sender_index) {
    pthread_mutex_lock(&room->chat_mtx);

    for(int i = 0; i < room->member_count; i++) {
        if (i == sender_index) {
            continue;
        }
        if (send(room->client_sockets.at(i), message, MAX_DATA, 0) < 0) {
            LOG(ERROR) << "Chat message failed to send";
        }
    }

    pthread_mutex_unlock(&room->chat_mtx);
}

void* room_client_listener(void* arg) {
    Room* room = (Room*) arg;
    char buff[MAX_DATA];
    int client_index = room->member_count - 1;
    int client_socket = room->client_sockets.at(client_index);

    LOG(INFO) << "Client listening on room " << room->name;

    while (recv(client_socket, &buff, MAX_DATA, 0) > 0) {
        // Send the message to every other client
        room_message(room, buff, client_index);
    }

    // TODO: implement checks for client exitting
    // close(client_socket);
    return NULL;
}

void* room_master_listener(void* arg) {
    Room* room = (Room*) arg;

    // listen socket
    if (listen(room->master_socket, MAX_MEMBER) < 0) {
        LOG(ERROR) << "ERROR: room could not listen on socket";
        exit(EXIT_FAILURE);
    }

    LOG(INFO) << "Room " << room->name << " ready for connections";

    int client_fd;
    struct sockaddr_in client_addr;
    int client_size = sizeof(struct sockaddr_in);
    while (true) {
        // accept a client
        if ((client_fd = accept(room->master_socket, (struct sockaddr*) &client_addr, (socklen_t*) &client_size)) < 0) {
            LOG(ERROR) << "ERROR: accept failed";
            continue;
        }

        LOG(INFO) << "Room " << room->name << " accepted client: " << client_fd;

        pthread_mutex_lock(&room_mtx);
        room->member_count++;

        room->client_sockets.push_back(client_fd);
        pthread_mutex_unlock(&room_mtx);

        LOG(INFO) << "Current number of room clients: " << room->member_count;

        pthread_t handler_thread;
        pthread_create(&handler_thread, NULL, &room_client_listener, &room);
    }

    return NULL;
}

Reply handle_create(char* buffer) {
    LOG(INFO) << "Create command received";
    // Create reply
    Reply reply;
    reply.status = SUCCESS;

    // Get the name from the buffer
    std::vector<char*> split_buffer = split(buffer, " ");
    if (split_buffer.size() < 2) {
        reply.status = FAILURE_INVALID;
        return reply;
    }

    char* name = split_buffer.at(1);

    // check if room is already created
    pthread_mutex_lock(&room_mtx);
    for (int i = 0; i < room_db.size(); i++) {
        if (strcmp(name, room_db[i]->name) == 0) {
            // Found a match -- return already exists
            pthread_mutex_unlock(&room_mtx);
            reply.status = FAILURE_ALREADY_EXISTS;
            return reply;
        }
    }
    pthread_mutex_unlock(&room_mtx);

    // open new master socket for the room
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    // create socket
    int fd;
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        LOG(ERROR) << "ERROR: could not open socket";
        exit(EXIT_FAILURE);
    }

    const int enable = 1;
    // set socket to allow reuse
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &enable, sizeof(int)) < 0) {
        LOG(ERROR) << "ERROR: setsockopt failed";
        exit(EXIT_FAILURE);
    }

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port); 

    // bind the socket
    if (bind(fd, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "ERROR: could not bind socket";
        exit(EXIT_FAILURE);
    }

    // Create a new room
    pthread_mutex_lock(&room_mtx);

    Room* new_room = (Room*) malloc(sizeof(Room));
    strcpy(new_room->name, name);
    new_room->member_count = 0;
    new_room->port = ++port;
    new_room->index = room_db.size();
    new_room->master_socket = fd;
    pthread_mutex_init(&new_room->chat_mtx, NULL);
    room_db.push_back(new_room);

    pthread_mutex_unlock(&room_mtx);

    LOG(INFO) << "Room " << new_room->name << " created successfully";
    // Create a thread to listen and accept connections for the room
    pthread_t thread;
    pthread_create(&thread, NULL, &room_master_listener, new_room);

    
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
    if (room_db.empty()) {
        strcpy(reply.list_room, "empty");
        return reply;
    }

    char list[MAX_DATA] = "";
    for (auto room : room_db) {
        strcat(list, room->name);
        strcat(list, ",");
    }
    strcpy(reply.list_room, list);
    return reply;
}

void parse_command(char* buffer, int fd) {
    Reply reply;
    char resp[MAX_DATA];

    // LOG(INFO) << "parse_command buffer: " << buffer;

    if (strncmp(buffer, "CREATE", 6) == 0){
        reply = handle_create(buffer);
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
        LOG(INFO) << "Received invalid command";
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
    if (listen(control_fd, 100) < 0) {
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

