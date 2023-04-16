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
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "followsync.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using snsFollowSync::SNSFollowSync;
using snsFollowSync::Relation;
using snsFollowSync::Users;
using snsFollowSync::Post;
using snsFollowSync::Reply;

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

void RunSync(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSFollowSyncImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "FollowSync listening on " << server_address << std::endl;
    log(INFO, "FollowSync listening on "+server_address);

    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "3010";

    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
            port = optarg;break;
            default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("followsync-") + port;

    // log to the terminal
    FLAGS_alsologtostderr = 1;

    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. FollowSync starting...");
    RunSync(port);

    return 0;
}
