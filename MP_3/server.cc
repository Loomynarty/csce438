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
#include <sys/stat.h>
#define glog(severity, msg) \
    LOG(severity) << msg;   \
    google::FlushLogFiles(google::severity);

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
ServerType type;
std::string follow_location = "follow.json";
std::string timeline_location = "timeline.json";

// Last update check
Timestamp last_update;

// Number of seconds for the master to check if any files have been updated
int update_time = 30;

// Slave info
std::string slave_info = "-1";

struct User
{
    std::string username;
    bool connected = false;
    std::vector<User *> followers;
    std::vector<User *> following;
    ServerReaderWriter<Message, Message> *stream = 0;
    bool operator==(const User &c1) const
    {
        return (username == c1.username);
    }
};

// Coordinator Stub
std::unique_ptr<SNSCoordinator::Stub> coord_stub_;

// Slave Stub
std::unique_ptr<SNSService::Stub> slave_stub_;

// Vector that stores every client that has been created
std::vector<User *> user_db;

// Helper function used to find a Client object given its username
int find_user(std::string username)
{
    int index = 0;
    for (User *c : user_db)
    {
        if (c->username == username)
            return index;
        index++;
    }
    return -1;
}

// Check if user -> follow_username
int find_following(User *user, std::string following_username)
{
    int index = 0;
    for (User *c : user->following)
    {
        if (c->username == following_username)
        {
            return index;
        }
        index++;
    }
    return -1;
}

void UpdateJSON(json j, std::string location)
{
    // glog(INFO, "Updating json");
    std::ofstream ofs(location);
    ofs << std::setw(4) << j << std::endl;
    ofs.close();
}

// Add a new user to data.json
void CreateUserJSON(std::string username)
{
    // Load data.json
    std::ifstream file(follow_location);
    json j = json::parse(file);
    file.close();

    // Create user object
    json user_data;
    user_data["username"] = username;
    user_data["following"] = json::object();
    j["users"][username] = user_data;

    UpdateJSON(j, follow_location);
}

void FollowUserJSON(std::string username, std::string username_to_follow)
{

    // Load data.json
    std::ifstream file(follow_location);
    json j = json::parse(file);
    file.close();

    Timestamp *timestamp = new Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);

    json follow_data;
    follow_data["username"] = username_to_follow;
    follow_data["timestamp"] = timestamp->seconds();

    // Update json
    j["users"][username]["following"][username_to_follow] = follow_data;

    UpdateJSON(j, follow_location);
}

void TimelineJSON(Message message, Timestamp *timestamp)
{
    std::string username = message.username();

    // Load data.json
    std::ifstream file(timeline_location);
    json j = json::parse(file);
    file.close();

    json post = json::object();
    post["message"] = message.msg();
    post["username"] = message.username();
    post["timestamp"] = timestamp->seconds();
    j["posts"].push_back(post);

    UpdateJSON(j, timeline_location);
}

void CreateEmptyData() {
    json fj = json::object();
    json tj = json::object();
    fj["users"] = json::object();
    tj["posts"] = json::array();
    UpdateJSON(fj, follow_location);
    UpdateJSON(tj, timeline_location);
}

// Load follow data - assumes empty local db
void LoadFollowData()
{
    std::ifstream file(follow_location);
    json j;

    if (file.is_open())
    {

        // Check if file is empty
        if (file.peek() == std::ifstream::traits_type::eof())
        {
            file.close();
            CreateEmptyData();
            return;
        }

        // Parse json
        j = json::parse(file);
        file.close();

        for (auto user_data : j["users"])
        {
            // Find user in local db
            User *user;
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
            else
            {
                user = user_db[user_index];
            }

            // Load followings / followers
            for (auto following_data : user_data["following"])
            {
                std::string follow_username = following_data["username"];
                User *user2;
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
                else
                {
                    user2 = user_db[index];
                }

                // Check if already following
                if ((find_following(user, follow_username)) < -1)
                {
                    glog(INFO, "Already Following");
                    continue;
                }

                user->following.push_back(user2);
                user2->followers.push_back(user);
            }
        }
    }

    // Create data.json if it doesn't exist
    else
    {
        CreateEmptyData();
    }
}

class SNSServiceImpl final : public SNSService::Service
{

    Status List(ServerContext *context, const Request *request, ListReply *list_reply) override
    {
        glog(INFO, "Serving List Request");
        User *user = user_db[find_user(request->username())];

        // Add all users in the db
        for (User *c : user_db)
        {
            list_reply->add_all_users(c->username);
        }

        // Add self to followers
        list_reply->add_followers(user->username);

        // Find users that are followers of user
        for (User *u : user->followers)
        {
            list_reply->add_followers(u->username);
        }

        return Status::OK;
    }

    Status Follow(ServerContext *context, const Request *request, Reply *reply) override
    {
        std::string username1 = request->username();
        std::string username2 = request->arguments(0);
        glog(INFO, "Serving Follow Request - " + username1 + " -> " + username2);

        // Copy operation to slave
        if (type == MASTER) {
            ClientContext ctx;
            Request req;
            req.set_username(username1);
            req.add_arguments(username2);
            Reply rep;
            slave_stub_->Follow(&ctx, req, &rep);
        }


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
        User *c;
        std::string username = request->username();

        // Catch SIGINT case - flip connected to false;
        if (!request->arguments().empty())
        {
            glog(INFO, "Client SIGINT - " + request->username());
            // Copy to slave
            if (type == MASTER) {
                ClientContext ctx;
                Request req;
                req.set_username(username);
                req.add_arguments("SIGINT");
                Reply rep;

                slave_stub_->Login(&ctx, req, &rep);
            }

            user_db[find_user(username)]->connected = false;
            return Status::CANCELLED;
        }

        glog(INFO, "Serving Login Request - " + request->username());

        // Copy operation to slave
        if (type == MASTER) {
            ClientContext ctx;
            Request req;
            req.set_username(username);
            Reply rep;
            slave_stub_->Login(&ctx, req, &rep);
        }

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

            if (user->connected)
                reply->set_msg("You have already logged in!");
            else
            {
                std::string msg = "Welcome Back " + user->username;
                reply->set_msg(msg);
                user->connected = true;
            }
        }
        glog(INFO, "Login Request - " + reply->msg());
        return Status::OK;
    }

    Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override
    {
        // ------------------------------------------------------------
        // In this function, you are to write code that handles
        // receiving a message/post from a user, recording it in a file
        // and then making it available on his/her follower's streams
        // ------------------------------------------------------------
        glog(INFO, "Serving Timeline Request");
        ClientContext ctx;
        std::shared_ptr<ClientReaderWriter<Message, Message>> slave_stream;
        
        // Copy operation to slave
        if (type == MASTER) {
            glog(INFO, "Setting up slave_stream";)
            slave_stream = std::shared_ptr<ClientReaderWriter<Message, Message>>(slave_stub_->Timeline(&ctx));
        }

        Message message_recv;
        Message message_send;
        std::string uname;
        int user_index = -1;
        bool init = true;

        while (stream->Read(&message_recv))
        {
            User *user;

            // Copy to slave
            if (type == MASTER) {
                glog(INFO, "Copying to slave";)
                slave_stream->Write(message_recv);
            }

            // Check if inital setup
            if (message_recv.msg() == "INIT" && init)
            {

                init = false;
                uname = message_recv.username();
                user_index = find_user(uname);
                user = user_db[user_index];

                if (user->stream == 0)
                {
                    user->stream = stream;
                }

                // Retrieve following messages - up to 20
                int count = 0;

                // Load json
                std::ifstream ffile(follow_location);
                std::ifstream tfile(timeline_location);
                json follow_json = json::parse(ffile);
                json time_json = json::parse(tfile);
                ffile.close();
                tfile.close();

                // Get the recent 20 messages
                json posts = time_json["posts"];
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
                        int64_t follow_age = follow_json["users"][uname]["following"][message_username]["timestamp"];
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
                    if (type == MASTER) {
                        stream->Write(message_send);
                    }
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

                if (type == MASTER) {
                    // send post to followers
                    for (User *u : user->followers)
                    {
                        if (u->stream != 0)
                        {
                            u->stream->Write(message_send);
                        }
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

void update_thread() {
    // Initialize last_update
    struct stat ffile_stat;
    time_t previous_follow_mtime = 0;

    if (stat(follow_location.c_str(), &ffile_stat) != 0) {
        glog(ERROR, "Stat for follow file failed");
        return;
    }
    previous_follow_mtime = ffile_stat.st_mtime;

    while (true) {
        // Sleep
        std::this_thread::sleep_for(std::chrono::seconds(update_time));
        glog(INFO, "Checking for updates...");

        // Check if follow file was updated
        if (stat(follow_location.c_str(), &ffile_stat) != 0) {
            glog(ERROR, "Stat for follow file failed");
            return;
        }
        if (previous_follow_mtime != ffile_stat.st_mtime) {
            // Reload follow data
            glog(INFO, "Follow file change detected - loading data");
            LoadFollowData();
            previous_follow_mtime = ffile_stat.st_mtime;
        }
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

    server->Wait();
}

int main(int argc, char **argv)
{

    // Coordinator default location
    std::string caddr = "0.0.0.0";
    std::string cport = "8000";
    std::string t = "-1";

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

    
    if (t == "master")
    {
        type = MASTER;
    }
    else if (t == "slave")
    {
        type = SLAVE;
    }

    // Create folders for this server
    std::string folder_name = t + "_" +  id;
    mkdir(folder_name.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    follow_location = folder_name + "/" + follow_location;
    timeline_location = folder_name + "/" + timeline_location;
    LoadFollowData();


    // Start heartbeat thread
    std::thread hb(heartbeat_thread, std::stoi(id), type, "0.0.0.0", port);

    // Start update thread
    std::thread update(update_thread);


    // Get the slave server
    if (type == MASTER) {
        ClientContext ctx;
        snsCoordinator::Server s;
        snsCoordinator::ClusterID cid;
        cid.set_cluster(std::stoi(id));
        Status status = coord_stub_->GetSlave(&ctx, cid, &s);

        if (!status.ok()) {
            glog(ERROR, "GetSlave failed");
            return 0;
        }

        slave_info = s.server_ip() + ":" + s.port_num();
        // glog(INFO, "Slave info: " + slave_info);
        slave_stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(grpc::CreateChannel(slave_info, grpc::InsecureChannelCredentials())));
    }

    // Start the server
    RunServer(port);
    hb.join();
    update.join();

    return 0;
}
