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
#include <algorithm>
#include "interface.h"

// Default port
int port = 8080;
int num_clients = 0;
std::vector<Room*> room_db;
pthread_mutex_t db_mtx = PTHREAD_MUTEX_INITIALIZER;

int delete_room(const char* target_name) {
    // Return if there are no rooms in the local db
    pthread_mutex_lock(&db_mtx);
    if (room_db.empty()) {
        pthread_mutex_unlock(&db_mtx);
        return -1;
    }

    for (auto it = room_db.begin(); it != room_db.end(); it++) {
        // Compare names of the rooms with the target name
        if (strcmp(target_name, (*it)->name) == 0) {
            // Found a match -- delete the room
            room_db.erase(it);
            pthread_mutex_unlock(&db_mtx);
            return 0;
        }
    }
    // No match found
    pthread_mutex_unlock(&db_mtx);
    return -1;
}

Room* find_room(const char* target_name) {
    // Return if there are no rooms in the local db
    pthread_mutex_lock(&db_mtx);
    if (room_db.empty()) {
        pthread_mutex_unlock(&db_mtx);
        return NULL;
    }

    for (auto room : room_db) {
        // Compare names of the rooms with the target name
        if (strcmp(target_name, room->name) == 0) {
            // Found a match -- return the room
            pthread_mutex_unlock(&db_mtx);
            return room;
        }
    }
    // No match found -- return NULL
    pthread_mutex_unlock(&db_mtx);
    return NULL;
}

void room_message(Room* room, const char* message, int sender_fd) {
    pthread_mutex_lock(&room->mtx);

    // Loop through the members of the room
    for (int socket : *(room->client_sockets)) {
        // Skip if the sender is the current index
        if (socket == sender_fd) {
            continue;
        }
        // Otherwise, send the message to the other client
        if (send(socket, message, MAX_DATA, 0) < 0) {
            LOG(ERROR) << "Chat message failed to send";
        }
    }

    pthread_mutex_unlock(&room->mtx);
}

void* room_client_listener(void* arg) {
    room_listener_t data = *(room_listener_t*) arg;
    char* room_name = data.name;
    Room* room = find_room(room_name);
    char buff[MAX_DATA];

    int client_socket = data.client_fd;
    LOG(INFO) << "Client " << client_socket << " connected to room " << room_name;
    LOG(INFO) << "Current number of room clients: " << room->member_count;

    while (recv(client_socket, &buff, MAX_DATA, 0) > 0) {
        // Send the message to every other client
        room_message(room, buff, client_socket);
    }

    LOG(INFO) << "Client " << client_socket << " left room " << room->name;
    // Remove client_socket from room client sockets
    pthread_mutex_lock(&room->mtx);
    room->client_sockets->remove(client_socket);
    pthread_mutex_unlock(&room->mtx);

    close(client_socket);
    room->member_count--;
    return NULL;
}

void* room_master_listener(void* arg) {
    char* room_name = (char*) arg;
    Room* room = find_room(room_name);

    // listen socket
    if (listen(room->master_socket, MAX_MEMBER) < 0) {
        LOG(ERROR) << "ERROR: room could not listen on socket";
        exit(EXIT_FAILURE);
    }

    LOG(INFO) << "Room " << room->name << " ready for connections on port " << room->port;

    int client_fd;
    struct sockaddr_in client_addr;
    int client_size = sizeof(struct sockaddr_in);
    while (true) {
        // accept a client
        if ((client_fd = accept(room->master_socket, (struct sockaddr*) &client_addr, (socklen_t*) &client_size)) < 0) {
            LOG(ERROR) << "ERROR: accept failed";
            continue;
        }

        // Add the client to the room member count and client sockets
        pthread_mutex_lock(&room->mtx);
        room->member_count++;
        room->client_sockets->push_back(client_fd);
        pthread_mutex_unlock(&room->mtx);



        // Create a new thread that will send the clients' messages to the others connected to the room
        room_listener_t data;
        strcpy(data.name, room_name);
        data.client_fd = client_fd;

        pthread_t handler_thread;
        pthread_create(&handler_thread, NULL, &room_client_listener, (void*) &data);
        room->threads.push_back(handler_thread);
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
    pthread_mutex_lock(&db_mtx);
    for (int i = 0; i < room_db.size(); i++) {
        if (strcmp(name, room_db[i]->name) == 0) {
            // Found a match -- return already exists
            pthread_mutex_unlock(&db_mtx);
            reply.status = FAILURE_ALREADY_EXISTS;
            return reply;
        }
    }
    pthread_mutex_unlock(&db_mtx);

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
    addr.sin_port = htons(++port); 

    // bind the socket
    if (bind(fd, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "ERROR: could not bind socket";
        exit(EXIT_FAILURE);
    }

    // Create a new room
    pthread_mutex_lock(&db_mtx);
    Room* new_room = (Room*) malloc(sizeof(Room));
    strcpy(new_room->name, name);
    new_room->member_count = 0;
    new_room->port = port;
    new_room->master_socket = fd;
    new_room->client_sockets = new std::list<int>;
    pthread_mutex_init(&new_room->mtx, NULL);
    room_db.push_back(new_room);
    pthread_mutex_unlock(&db_mtx);    

    // Create a thread to listen and accept connections for the room
    pthread_t thread;
    pthread_create(&thread, NULL, &room_master_listener, (void*) new_room->name);

    return reply;
}

Reply handle_delete(char* buffer) {
    LOG(INFO) << "Delete command received";
    // Create reply and send
    Reply reply;
    reply.status = SUCCESS;
    
    // Get the name from the buffer
    std::vector<char*> split_buffer = split(buffer, " ");
    if (split_buffer.size() < 2) {
        reply.status = FAILURE_INVALID;
        return reply;
    }
    char* name = split_buffer.at(1);
    Room* room = find_room(name);
    if (room == NULL) {
        reply.status = FAILURE_NOT_EXISTS;
        return reply;
    }

    // Send warning message to all connected room clients
    room_message(room, CLOSE_MESSAGE, -1);

    // Close client connections to the room
    pthread_mutex_lock(&room->mtx);
    for (auto socket : *(room->client_sockets)) {
        close(socket);
    }

    // Delete the room and free memory allocated
    delete room->client_sockets;
    for (auto thread : room->threads) {
        pthread_cancel(thread);
    }
    pthread_mutex_unlock(&room->mtx);

    delete_room(name);
    free(room);

    return reply;
}

Reply handle_join(char* buffer) {
    LOG(INFO) << "Join command received";
    // Create reply and send
    Reply reply;
    reply.status = SUCCESS;
    
    // Get the name from the buffer
    std::vector<char*> split_buffer = split(buffer, " ");
    if (split_buffer.size() < 2) {
        reply.status = FAILURE_INVALID;
        return reply;
    }
    char* name = split_buffer.at(1);

    // check if room with the name provided exists
    Room* room = find_room(name);
    if (room == NULL) {
        reply.status = FAILURE_NOT_EXISTS;
        return reply;
    }

    reply.num_member = room->member_count + 1;
    reply.port = room->port;

    return reply;
}

Reply handle_list(char* buffer) {
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
    bool is_join = false;

    if (strncmp(buffer, "CREATE", 6) == 0){
        reply = handle_create(buffer);
    } 
    else if (strncmp(buffer, "DELETE", 6) == 0){
        reply = handle_delete(buffer);
    } 
    else if (strncmp(buffer, "JOIN", 4) == 0){
        reply = handle_join(buffer);
        is_join = true;
    } 
    else if (strncmp(buffer, "LIST", 4) == 0){
        reply = handle_list(buffer);
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
    // Close the client socket if a JOIN command was issued
    if (is_join && reply.status == SUCCESS) {
        close(fd);
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

