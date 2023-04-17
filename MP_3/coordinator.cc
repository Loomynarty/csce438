#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <vector>
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
using grpc::Status;
using snsCoordinator::SNSCoordinator;
using snsCoordinator::Heartbeat;
using snsCoordinator::User;
using snsCoordinator::Users;
using snsCoordinator::ClusterID;
using snsCoordinator::FollowSyncs;
using google::protobuf::util::TimeUtil;

// Store Master servers
// Store Slave servers
// Store FollowSyncs

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
    
    Status HandleHeartBeats(ServerContext* context, ServerReaderWriter<Heartbeat, Heartbeat>* stream) override {
        Heartbeat beat;
        while (stream->Read(&beat)) {
            log(INFO, "Received Heartbeat - Server " + std::to_string(beat.server_id()));
            // log(INFO, "Server ID: " + std::to_string(beat.server_id()));
            // log(INFO, "Server Type: " + std::to_string(beat.server_type()));
            // log(INFO, "Server Location: " + beat.server_ip() + ":" + beat.server_port());
            // log(INFO, "Timestamp: " + TimeUtil::ToString(beat.timestamp()));


            // a) if first heartbeat, add to master / slave server db 
            // create a timer for 20 seconds tied to the server
            // reset it if another heartbeat is received
            // if the 20 second timer pops, assume dead and either mark it as dead or remove from db. return State:Bad or something
        }

        return Status::OK;
    }

    Status GetFollowSyncsForUsers(ServerContext* context, const Users* users, FollowSyncs* syncs) override {
        log(INFO, "Fetching followsync...");
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const User* user, snsCoordinator::Server server) {
        log(INFO, "Fetching serving...");
        return Status::OK;
    }

    Status GetSlave(ServerContext*, const ClusterID* cid, Server* server) {
        log(INFO, "Fetching slave...");
        return Status::OK;
    }


};

void RunCoordinator(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSCoordinatorImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Coordinator listening on " << server_address << std::endl;
    log(INFO, "Coordinator listening on "+server_address);

    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "8000";

    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
            port = optarg;break;
            default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("coordinator-") + port;

    // log to the terminal
    FLAGS_alsologtostderr = 1;

    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Coordinator starting...");
    RunCoordinator(port);

    return 0;
}
