#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

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
#include <thread>
#define glog(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "followsync.grpc.pb.h"
#include "json.hpp"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using snsFollowSync::SNSFollowSync;
using snsFollowSync::Relation;
using snsFollowSync::Users;
using snsFollowSync::Post;
using snsFollowSync::Reply;
using snsCoordinator::SNSCoordinator;
using snsCoordinator::Heartbeat;
using google::protobuf::util::TimeUtil;
using snsCoordinator::ServerType;
using snsCoordinator::MASTER;
using snsCoordinator::SLAVE;
using snsCoordinator::SYNC;
using json = nlohmann::ordered_json;

// Coordinator stub
std::unique_ptr<SNSCoordinator::Stub> coord_stub_;

// Number of seconds for the syncer to check if any files have been updated
int update_time = 10;

std::string follow_location = "follow.json";
std::string timeline_location = "timeline.json";
std::string master_follow_location;
std::string slave_follow_location;
std::string master_timeline_location;
std::string slave_timeline_location;

class SNSFollowSyncImpl final : public SNSFollowSync::Service {
    
    Status SyncUsers(ServerContext* context, const Users* users, Reply* reply) override {

        return Status::OK;
    }

    Status SyncRelations(ServerContext* context, const Relation* relation, Reply* reply) {

        return Status::OK;
    }

    Status SyncTimeline(ServerContext*, const Post* post, Reply* reply) {
        return Status::OK;
    }

};

void send_heartbeat(int id, ServerType type, std::string ip, std::string port) {
    // Create the stream
    ClientContext ctx;
    std::shared_ptr<ClientReaderWriter<Heartbeat, Heartbeat>> stream(coord_stub_->HandleHeartBeats(&ctx));

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
    stream->WritesDone();
    stream->Finish();
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

std::vector<int> ParseUsers(std::string location)
{
    std::vector<int> users;
    std::ifstream file(location);
    json j;

    if (file.is_open())
    {

        // Check if file is empty
        if (file.peek() == std::ifstream::traits_type::eof())
        {
            file.close();
            return users;
        }

        // Parse json
        j = json::parse(file);
        file.close();

        for (auto user_data : j["users"])
        {
            // Add all users to vector
            std::string username = user_data["username"];
            users.push_back(std::stoi(username));
        }
    }

    return users;
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
            glog(INFO, "Follow file change detected");
            // std::vector<int> users = ParseUsers(master_follow_location);

            previous_follow_mtime = ffile_stat.st_mtime;
        }
    }

}


void RunSync(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSFollowSyncImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "FollowSync listening on " << server_address << std::endl;
    glog(INFO, "FollowSync listening on "+server_address);

    server->Wait();
}

int main(int argc, char** argv) {

    // Coordinator default location
    std::string caddr = "0.0.0.0";
    std::string cport = "8000";
    std::string port = "-1";
    std::string id = "-1";

    int opt = 0;
    while ((opt = getopt(argc, argv, "c:o:p:i:")) != -1){
        switch (opt) {
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

    std::string log_file_name = std::string("followsync-") + port;

    // log to the terminal
    FLAGS_alsologtostderr = 1;

    google::InitGoogleLogging(log_file_name.c_str());


    // Set folders for this syncer
    std::string master_folder =  "master_" +  id;
    std::string slave_folder =  "slave_" +  id;
    master_follow_location = master_folder + "/" + follow_location;
    slave_follow_location = slave_folder + "/" + follow_location;
    master_timeline_location = master_folder + "/" + timeline_location;
    slave_timeline_location = slave_folder + "/" + timeline_location;

    // Create coordinator stub
    std::string coord_login = caddr + ":" + cport;
    coord_stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(grpc::CreateChannel(coord_login, grpc::InsecureChannelCredentials())));

    // Send init heartbeat to coordinator
    send_heartbeat(std::stoi(id), SYNC, "0.0.0.0", port);

    glog(INFO, "Logging Initialized. FollowSync starting...");
    RunSync(port);


    return 0;
}
