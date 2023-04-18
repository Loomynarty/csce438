/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <thread>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using snsCoordinator::Heartbeat;
using snsCoordinator::SNSCoordinator;
using snsCoordinator::ServerType;
using snsCoordinator::MASTER;
using snsCoordinator::SLAVE;
using snsCoordinator::SYNC;
using google::protobuf::util::TimeUtil;

struct Client {
    std::string username;
    bool connected = true;
    int following_file_size = 0;
    std::vector<Client*> client_followers;
    std::vector<Client*> client_following;
    ServerReaderWriter<Message, Message>* stream = 0;
    bool operator==(const Client& c1) const{
        return (username == c1.username);
    }
};

// Coordinator Stub
std::unique_ptr<SNSCoordinator::Stub> coord_stub_;

//Vector that stores every client that has been created
std::vector<Client> client_db;

//Helper function used to find a Client object given its username
int find_user(std::string username){
    int index = 0;
    for(Client c : client_db){
        if(c.username == username)
            return index;
        index++;
    }
    return -1;
}

class SNSServiceImpl final : public SNSService::Service {
  
    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        log(INFO,"Serving List Request");
        Client user = client_db[find_user(request->username())];
        int index = 0;
        for(Client c : client_db){
            list_reply->add_all_users(c.username);
        }
        std::vector<Client*>::const_iterator it;
        for(it = user.client_followers.begin(); it!=user.client_followers.end(); it++){
            list_reply->add_followers((*it)->username);
        }
        return Status::OK;
    }

    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string username1 = request->username();
        std::string username2 = request->arguments(0);
        log(INFO,"Serving Follow Request - " + username1 + " -> " + username2);

        int join_index = find_user(username2);
        if (join_index < 0 || username1 == username2) {
            reply->set_msg("Follow Failed - Invalid Username");
        }
        else {
            Client *user1 = &client_db[find_user(username1)];
            Client *user2 = &client_db[join_index];

            if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) != user1->client_following.end()) {
                reply->set_msg("Follow Failed - Already Following User");
                log(INFO, "Follow Request - " + reply->msg());
                return Status::OK;
            }

            user1->client_following.push_back(user2);
            user2->client_followers.push_back(user1);
            reply->set_msg("Follow Successful");
        }

        // log(INFO, "Follow Request - " + reply->msg());
        return Status::OK; 
    }
  
    Status Login(ServerContext* context, const Request* request, Reply* reply) override {
        log(INFO,"Serving Login Request - " + request->username());
        Client c;
        std::string username = request->username();
        int user_index = find_user(username);
        if(user_index < 0){
            c.username = username;
            client_db.push_back(c);
            reply->set_msg("Login Successful!");
        }
        else{ 
            Client *user = &client_db[user_index];
            if(user->connected)
                reply->set_msg("You have already logged in!");
            else{
                std::string msg = "Welcome Back " + user->username;
                reply->set_msg(msg);
                user->connected = true;
            }
        }
        log(INFO, "Login Request - " + reply->msg());
        return Status::OK;
    }

    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
        log(INFO,"Serving Timeline Request");
        Message message;
        Client *c;
        while(stream->Read(&message)) {
            std::string username = message.username();
            int user_index = find_user(username);
            c = &client_db[user_index];

            //Write the current message to "username.txt"
            std::string filename = username+".txt";
            std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
            Timestamp temptime = message.timestamp();
            std::string time = TimeUtil::ToString(temptime);
            std::string fileinput = time+" :: "+message.username()+":"+message.msg()+"\n";

            //"Set Stream" is the default message from the client to initialize the stream
            if(message.msg() != "Set Stream")
            user_file << fileinput;

            //If message = "Set Stream", print the first 20 chats from the people you follow
            else{
                if(c->stream==0)
                    c->stream = stream;
                std::string line;
                std::vector<std::string> newest_twenty;
                std::ifstream in(username+"following.txt");
                int count = 0;

                //Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
                while(getline(in, line)) {
                    if(c->following_file_size > 20){
                        if(count < c->following_file_size-20){
                            count++;
                            continue;
                        }
                    }
                    newest_twenty.push_back(line);
                }
                Message new_msg;

                //Send the newest messages to the client to be displayed
                for(int i = 0; i<newest_twenty.size(); i++){
                    new_msg.set_msg(newest_twenty[i]);
                    stream->Write(new_msg);
                }    
                continue;
            }

            //Send the message to each follower's stream
            std::vector<Client*>::const_iterator it;
            for(it = c->client_followers.begin(); it!=c->client_followers.end(); it++){
                Client *temp_client = *it;
                if(temp_client->stream!=0 && temp_client->connected)
                temp_client->stream->Write(message);
                
                //For each of the current user's followers, put the message in their following.txt file
                std::string temp_username = temp_client->username;
                std::string temp_file = temp_username + "following.txt";
                std::ofstream following_file(temp_file,std::ios::app|std::ios::out|std::ios::in);
                following_file << fileinput;
                temp_client->following_file_size++;
                std::ofstream user_file(temp_username + ".txt",std::ios::app|std::ios::out|std::ios::in);
                user_file << fileinput;
            }
        }
        //If the client disconnected from Chat Mode, set connected to false
        c->connected = false;
        return Status::OK;
    }

};

void heartbeat_thread(int id, ServerType type, std::string ip, std::string port) {

    // Create the stream
    ClientContext ctx;
    std::shared_ptr<ClientReaderWriter<Heartbeat, Heartbeat>> stream(coord_stub_->HandleHeartBeats(&ctx));

    while (true) {
        // Create heartbeat
        log(INFO, "Sending heartbeat");

        Heartbeat beat;
        beat.set_server_id(id);
        beat.set_server_type(type);
        beat.set_server_ip(ip);
        beat.set_server_port(port);
        Timestamp* timestamp = new Timestamp();
        timestamp->set_seconds(time(NULL));
        timestamp->set_nanos(0);
        beat.set_allocated_timestamp(timestamp);

        // Send to coordinator
        stream->Write(beat); 

        // Sleep for 10 seconds
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

void RunServer(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Server listening on "+server_address);

    server->Wait();
}

int main(int argc, char** argv) {

    // Coordinator default location
    std::string caddr = "0.0.0.0";
    std::string cport = "8000";

    std::string port = "-1";
    std::string id = "-1";
    std::string t = "-1";
    int opt = 0;
    while ((opt = getopt(argc, argv, "c:o:p:i:t:")) != -1){
        switch(opt) {
            case 'c':
                caddr = optarg;
                break;
            case 'o':
                cport = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'i':
                id = optarg;
                break;
            case 't':
                t = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    if (port == "-1") {
        std::cout << "Please enter a port! (-p)";
        return -1;
    }
    if (id == "-1") {
        std::cout << "Please enter an id! (-i)";
        return -1;
    }
    if (t != "master" && t != "slave") {
        std::cout << "Please enter a valid type! (-t)";
        return -1;
    }

    std::string log_file_name = t + id + "-" + port;

    // log to the terminal
    FLAGS_alsologtostderr = 1;

    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    // Create coordinator stub
    std::string coord_login = caddr + ":" + cport;
    coord_stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(grpc::CreateChannel(coord_login, grpc::InsecureChannelCredentials())));

    ServerType type;
    if (t == "master") {
        type = MASTER;
    }
    else if (t == "slave") {
        type = SLAVE;
    }

    // Start heartbeat thread
    std::thread hb(heartbeat_thread, std::stoi(id), type, "0.0.0.0", port);

    // Start the server
    RunServer(port);
    hb.join();

    return 0;
}
