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
#define glog(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "json.hpp"

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using snsCoordinator::Heartbeat;
using snsCoordinator::MASTER;
using snsCoordinator::ServerType;
using snsCoordinator::SLAVE;
using snsCoordinator::SNSCoordinator;
using snsCoordinator::SYNC;
using json = nlohmann::ordered_json;

// Server info
std::string port = "-1";
std::string id = "-1";
std::string t = "-1";
std::string data_location = "data.json";

struct User
{
    std::string username;
    bool connected = true;
    std::vector<User*> followers;
    std::vector<User*> following;
    ServerReaderWriter<Message, Message> *stream = 0;
    bool operator==(const User &c1) const
    {
        return (username == c1.username);
    }
};

// Coordinator Stub
std::unique_ptr<SNSCoordinator::Stub> coord_stub_;

// Vector that stores every client that has been created
std::vector<User*> user_db;

// Helper function used to find a Client object given its username
int find_user(std::string username)
{
    int index = 0;
    for (User* c : user_db)
    {
        if (c->username == username)
            return index;
        index++;
    }
    return -1;
}

// Check if user -> follow_username
int find_following(User* user, std::string following_username)
{   
    int index = 0;
    for (User* c : user->following)
    {
        if (c->username == following_username)
        {
            return index;
        }
        index++;
    }
    return -1;
}

void UpdateJSON(json j)
{
    std::ofstream ofs(data_location);
    ofs << std::setw(4) << j << std::endl;
    ofs.close();
}

// Add a new user to data.json
void CreateUserJSON(std::string username) {
  // std::cout << "adding " << username << " to storage\n";
  
  // Load data.json
  std::ifstream file(data_location);
  json j = json::parse(file);
  file.close();

  // Create user object
  json user_data;
  user_data["username"] = username;
  user_data["following"] = json::object();
  j["users"][username] = user_data;

  UpdateJSON(j);
}

void FollowUserJSON(std::string username, std::string username_to_follow) {
  // std::cout << username << " -> " << username_to_follow << " to storage\n";
  
  // Load data.json
  std::ifstream file(data_location);
  json j = json::parse(file);
  file.close();
  
  Timestamp* timestamp = new Timestamp();
  timestamp->set_seconds(time(NULL));
  timestamp->set_nanos(0);

  json follow_data;
  follow_data["username"] = username_to_follow;
  follow_data["timestamp"] = timestamp->seconds();

  // Update json
  j["users"][username]["following"][username_to_follow] = follow_data; 
  
  UpdateJSON(j);
}

void TimelineJSON(Message message, Timestamp* timestamp) {
  // std::cout << "writing post to storage\n";
  std::string username = message.username();

  // Load data.json
  std::ifstream file(data_location);
  json j = json::parse(file);
  file.close();

  json post = json::object();
  post["message"] = message.msg();
  post["username"] = message.username();
  post["timestamp"] = timestamp->seconds();
  j["posts"].push_back(post);

  UpdateJSON(j);
}


// Load inital data - assumes empty local db
void LoadInitialData()
{
    std::ifstream file(data_location);
    json j;

    if (file.is_open())
    {

        // Check if file is empty
        if (file.peek() == std::ifstream::traits_type::eof())
        {
            file.close();

            // Update data.json with an empty array
            j = json::object();
            j["users"] = json::object();
            j["posts"] = json::array();
            UpdateJSON(j);

            return;
        }

        // Parse json
        j = json::parse(file);
        file.close();

        for (auto user_data : j["users"])
        {
            // Find user in local db
            User* user;
            std::string uname = user_data["username"];
            int user_index = find_user(uname);

            // Create the user if not found
            if (user_index == -1)
            {   
                user = new User;
                user->username = uname;
                user_db.push_back(user);
            }
            // Grab the user in the local database
            else {
                user = user_db[user_index];
            }

            // Load followings / followers
            for (auto following_data : user_data["following"])
            {
                std::string follow_username = following_data["username"];
                User* user2;
                glog(INFO, "Adding " + uname + " -> " + follow_username);

                // Find user_to_follow in local db
                int index = find_user(follow_username);

                // Create the user if not found
                if (index == -1)
                {
                    user2 = new User;
                    user2->username = follow_username;
                    user_db.push_back(user2);
                }
                // Grab the user in the local database
                else {
                    user2 = user_db[index];
                }


                // Check if already following
                if ((find_following(user, follow_username)) < -1) {
                    glog(INFO, "Already Following");
                    continue;
                }

                user->following.push_back(user2);
                user2->followers.push_back(user);

                glog(INFO, "user_db: " + std::to_string(user_db.size()));
                std::cout << user_db.at(0)->username << " ";
                std::cout << user_db.at(0)->followers.size() << " ";
                std::cout << user_db.at(0)->following.size() << "\n";
                std::cout << user_db.at(1)->username << " ";
                std::cout << user_db.at(1)->followers.size() << " ";
                std::cout << user_db.at(1)->following.size() << "\n";
            }
        }
    }

    // Create data.json if it doesn't exist
    else
    {
        j = json::object();
        j["users"] = json::object();
        j["posts"] = json::array();
        UpdateJSON(j);
    }
}

class SNSServiceImpl final : public SNSService::Service
{

    Status List(ServerContext *context, const Request *request, ListReply *list_reply) override
    {
        glog(INFO, "Serving List Request");
        User* user = user_db[find_user(request->username())];
        glog(INFO, user->followers.size());
        glog(INFO, user->following.size());

        // Add all users in the db
        for (User* c : user_db)
        {
            glog(INFO, "Users Found: " + c->username);
            list_reply->add_all_users(c->username);
        }

        // Find users that are followers of user
        for (User* u : user->followers) {
            glog(INFO, "Followers: " + u->username);
            list_reply->add_followers(u->username);
        }

        return Status::OK;
    }

    Status Follow(ServerContext *context, const Request *request, Reply *reply) override
    {
        std::string username1 = request->username();
        std::string username2 = request->arguments(0);
        glog(INFO, "Serving Follow Request - " + username1 + " -> " + username2);

        int join_index = find_user(username2);
        
        // Prevent self follow or a user that isn't in the db
        if (join_index < 0 || username1 == username2)
        {
            reply->set_msg("Follow Failed - Invalid Username");
        }
        else
        {
            User *user1 = user_db[find_user(username1)];
            User *user2 = user_db[join_index];

            if (std::find(user1->following.begin(), user1->following.end(), user2) != user1->following.end())
            {
                reply->set_msg("Follow Failed - Already Following User");
                glog(INFO, "Follow Request - " + reply->msg());
                return Status::OK;
            }

            user1->following.push_back(user2);
            user2->followers.push_back(user1);
            reply->set_msg("Follow Successful");

            // Update json
            FollowUserJSON(user1->username, user2->username);
        }

        // log(INFO, "Follow Request - " + reply->msg());
        return Status::OK;
    }

    Status Login(ServerContext *context, const Request *request, Reply *reply) override
    {
        glog(INFO, "Serving Login Request - " + request->username());
        User* c;
        std::string username = request->username();
        int user_index = find_user(username);
        if (user_index < 0)
        {   
            c = new User;
            c->username = username;
            user_db.push_back(c);
            reply->set_msg("Login Successful!");

            // Update json
            CreateUserJSON(username);
        }
        else
        {
            User *user = user_db[user_index];

            // TODO - temp fix
            user->connected = false;

            if (user->connected)
                reply->set_msg("You have already logged in!");
            else
            {
                std::string msg = "Welcome Back " + user->username;
                reply->set_msg(msg);
                user->connected = true;
            }
        }
        // log(INFO, "Login Request - " + reply->msg());
        return Status::OK;
    }

    /*

    Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override
    {
        log(INFO, "Serving Timeline Request");
        Message message;
        User* c;
        while (stream->Read(&message))
        {
            std::string username = message.username();
            int user_index = find_user(username);
            c = &user_db[user_index];

            // Write the current message to "username.txt"
            std::string filename = username + ".txt";
            std::ofstream user_file(filename, std::ios::app | std::ios::out | std::ios::in);
            Timestamp temptime = message.timestamp();
            std::string time = TimeUtil::ToString(temptime);
            std::string fileinput = time + " :: " + message.username() + ":" + message.msg() + "\n";

            //"Set Stream" is the default message from the client to initialize the stream
            if (message.msg() != "Set Stream")
                user_file << fileinput;

            // If message = "Set Stream", print the first 20 chats from the people you follow
            else
            {
                if (c->stream == 0)
                    c->stream = stream;
                std::string line;
                std::vector<std::string> newest_twenty;
                std::ifstream in(username + "following.txt");
                int count = 0;

                // Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
                while (getline(in, line))
                {
                    if (c->following_file_size > 20)
                    {
                        if (count < c->following_file_size - 20)
                        {
                            count++;
                            continue;
                        }
                    }
                    newest_twenty.push_back(line);
                }
                Message new_msg;

                // Send the newest messages to the client to be displayed
                for (int i = 0; i < newest_twenty.size(); i++)
                {
                    new_msg.set_msg(newest_twenty[i]);
                    stream->Write(new_msg);
                }
                continue;
            }

            // Send the message to each follower's stream
            std::vector<Client *>::const_iterator it;
            for (it = c->client_followers.begin(); it != c->client_followers.end(); it++)
            {
                Client *temp_client = *it;
                if (temp_client->stream != 0 && temp_client->connected)
                    temp_client->stream->Write(message);

                // For each of the current user's followers, put the message in their following.txt file
                std::string temp_username = temp_client->username;
                std::string temp_file = temp_username + "following.txt";
                std::ofstream following_file(temp_file, std::ios::app | std::ios::out | std::ios::in);
                following_file << fileinput;
                temp_client->following_file_size++;
                std::ofstream user_file(temp_username + ".txt", std::ios::app | std::ios::out | std::ios::in);
                user_file << fileinput;
            }
        }

        // If the client disconnected from Chat Mode, set connected to false
        c->connected = false;
        return Status::OK;
    }

    */

    Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override
    {
        // ------------------------------------------------------------
        // In this function, you are to write code that handles
        // receiving a message/post from a user, recording it in a file
        // and then making it available on his/her follower's streams
        // ------------------------------------------------------------
        glog(INFO, "Serving Timeline Request");
        Message message_recv;
        Message message_send;
        std::string uname;
        int user_index = -1;
        bool init = true;

        while (stream->Read(&message_recv))
        {
            User *user;

            // Check if inital setup
            if (message_recv.msg() == "INIT" && init)
            {

                init = false;
                uname = message_recv.username();
                std::cout << uname << "\n";
                user_index = find_user(uname);
                user = user_db[user_index];

                if (user->stream == 0)
                {
                    user->stream = stream;
                }

                // Retrieve following messages - up to 20
                int count = 0;

                // Load json
                std::ifstream file(data_location);
                json j = json::parse(file);
                file.close();

                // Get the recent 20 messages
                json posts = j["posts"];
                for (auto it = posts.rbegin(); it != posts.rend(); it++)
                {
                    if (count >= 20)
                    {
                        break;
                    }

                    std::string message_username = (*it)["username"];
                    if (uname != message_username)
                    {
                        // Check if username is followed
                        if (find_following(user, message_username) < 0)
                        {
                            continue;
                        }

                        // Check if message is after follow age
                        int64_t follow_age = j["users"][uname]["following"][message_username]["timestamp"];
                        int64_t message_seconds = (*it)["timestamp"];

                        if (follow_age > message_seconds)
                        {
                            continue;
                        }
                    }

                    // Create message
                    message_send.set_username((*it)["username"]);
                    message_send.set_msg((*it)["message"]);
                    Timestamp *timestamp = new Timestamp();
                    timestamp->set_seconds((*it)["timestamp"]);
                    timestamp->set_nanos(0);
                    message_send.set_allocated_timestamp(timestamp);

                    // Send to client
                    stream->Write(message_send);
                    count++;
                }
            }

            // Send post to followers
            else
            {
                std::string str = message_recv.msg();

                // Create message
                message_send.set_username(uname);
                message_send.set_msg(str);
                Timestamp *timestamp = new Timestamp();
                timestamp->set_seconds(time(NULL));
                timestamp->set_nanos(0);
                message_send.set_allocated_timestamp(timestamp);

                // send post to followers
                for (User *u : user->followers)
                {
                    if (u->stream != 0)
                    {
                        u->stream->Write(message_send);
                    }
                }

                // Update JSON
                TimelineJSON(message_send, timestamp);
            }
        }

        return Status::OK;
    }
};

void heartbeat_thread(int id, ServerType type, std::string ip, std::string port)
{

    // Create the stream
    ClientContext ctx;
    std::shared_ptr<ClientReaderWriter<Heartbeat, Heartbeat>> stream(coord_stub_->HandleHeartBeats(&ctx));

    while (true)
    {
        // Create heartbeat
        glog(INFO, "Sending heartbeat");

        Heartbeat beat;
        beat.set_server_id(id);
        beat.set_server_type(type);
        beat.set_server_ip(ip);
        beat.set_server_port(port);
        Timestamp *timestamp = new Timestamp();
        timestamp->set_seconds(time(NULL));
        timestamp->set_nanos(0);
        beat.set_allocated_timestamp(timestamp);

        // Send to coordinator
        stream->Write(beat);

        // Sleep for 10 seconds
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

void RunServer(std::string port_no)
{
    std::string server_address = "0.0.0.0:" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    glog(INFO, "Server listening on " + server_address);

    LoadInitialData();

    server->Wait();
}

int main(int argc, char **argv)
{

    // Coordinator default location
    std::string caddr = "0.0.0.0";
    std::string cport = "8000";

    int opt = 0;
    while ((opt = getopt(argc, argv, "c:o:p:i:t:")) != -1)
    {
        switch (opt)
        {
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
    if (port == "-1")
    {
        std::cout << "Please enter a port! (-p)";
        return -1;
    }
    if (id == "-1")
    {
        std::cout << "Please enter an id! (-i)";
        return -1;
    }
    if (t != "master" && t != "slave")
    {
        std::cout << "Please enter a valid type! (-t)";
        return -1;
    }

    std::string log_file_name = t + id + "-" + port;

    // log to the terminal
    FLAGS_alsologtostderr = 1;

    google::InitGoogleLogging(log_file_name.c_str());
    glog(INFO, "Logging Initialized. Server starting...");

    // Create coordinator stub
    std::string coord_login = caddr + ":" + cport;
    coord_stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(grpc::CreateChannel(coord_login, grpc::InsecureChannelCredentials())));

    ServerType type;
    if (t == "master")
    {
        type = MASTER;
    }
    else if (t == "slave")
    {
        type = SLAVE;
    }

    // Start heartbeat thread
    std::thread hb(heartbeat_thread, std::stoi(id), type, "0.0.0.0", port);

    // Start the server
    RunServer(port);
    hb.join();

    return 0;
}
