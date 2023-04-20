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
// using grpc::Server;
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
using snsCoordinator::ServerType;
using snsCoordinator::MASTER;
using snsCoordinator::SLAVE;
using snsCoordinator::SYNC;
using snsCoordinator::Server;
using google::protobuf::util::TimeUtil;


struct server_t {
    int server_id;
    std::string ip;
    std::string port;
    ServerType type;
    Timestamp timestamp;
    bool active = true;
};

// Store Master servers
std::vector<server_t> master_table;

// Store Slave servers
std::vector<server_t> slave_table;

// Store FollowSyncs
std::vector<server_t> sync_table;

int find_server(int id, ServerType type) {
    int index = 0;
    switch(type) {
        case MASTER:
            for(server_t s : master_table) {
                if(s.server_id == id) {
                    return index;
                }
                index++;
            }
            break;

        case SLAVE:
            for(server_t s : slave_table) {
                if(s.server_id == id) {
                    return index;
                }
                index++;
            }
            break;

        case SYNC:
            for(server_t s : sync_table) {
                if(s.server_id == id) {
                    return index;
                }
                index++;
            }
            break;
    }
    return -1;
}

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
    
    Status HandleHeartBeats(ServerContext* context, ServerReaderWriter<Heartbeat, Heartbeat>* stream) override {
        log(INFO, "Connected masters: " + std::to_string(master_table.size()))
        Heartbeat beat;
        while (stream->Read(&beat)) {
            switch(beat.server_type()) {
                case MASTER:
                    log(INFO, "Received Heartbeat - Master " + std::to_string(beat.server_id()));
                    break;
                case SLAVE:
                    log(INFO, "Received Heartbeat - Slave " + std::to_string(beat.server_id()));
                    break;
                default:
                    log(INFO, "Received Heartbeat");
                    break;
            }

            // a) if first heartbeat, add to master / slave server db 
            // create a timer for 20 seconds tied to the server
            // reset it if another heartbeat is received
            // if the 20 second timer pops, assume dead and either mark it as dead or remove from db. return State:Bad or something

            int index = find_server(beat.server_id(), beat.server_type());
            if (index < 0) {
                log(INFO, "Adding new server to table");
                server_t s;
                s.server_id = beat.server_id();
                s.ip = beat.server_ip();
                s.port = beat.server_port();
                s.type = beat.server_type();
                s.timestamp = beat.timestamp();

                // Add server into the table
                switch(beat.server_type()) {
                    case MASTER:
                        master_table.push_back(s);
                        break;
                    case SLAVE:
                        slave_table.push_back(s);
                        break;
                    case SYNC:
                        sync_table.push_back(s);
                        break;
                }
            }
            else {
                // Update timestamp of existing server to new timestamp
                int index = find_server(beat.server_id(), beat.server_type());
                switch(beat.server_type()) {
                    case MASTER:
                        master_table.at(index).timestamp = beat.timestamp();
                        break;
                    case SLAVE:
                        slave_table.at(index).timestamp = beat.timestamp();
                        break;
                    case SYNC:
                        master_table.at(index).timestamp = beat.timestamp();
                        break;
                }
            }
        }

        return Status::OK;
    }

    Status GetFollowSyncsForUsers(ServerContext* context, const Users* users, FollowSyncs* syncs) override {
        log(INFO, "Fetching followsyncs...");
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const User* user, Server* server) {

        int id = user->user_id() % 3;
        log(INFO, "Fetching server... id " + std::to_string(id));
        // TODO - Risky access
        server_t s = master_table.at(id);

        server->set_server_ip(s.ip);   
        server->set_port_num(s.port);
        server->set_server_id(id + 1);
        server->set_server_type(s.type);   

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
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
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
