#include <iostream>
#include <string>
#include <unistd.h>
#include <thread>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"

using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using csce438::Message;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

// TODO - signal(SIGINT, handler)

class Client : public IClient
{
    public:
        Client(const std::string& hname,
               const std::string& uname,
               const std::string& p)
            :hostname(hname), username(uname), port(p)
            {}
    protected:
        virtual int connectTo();
        virtual IReply processCommand(std::string& input);
        virtual void processTimeline();
    private:
        std::string hostname;
        std::string username;
        std::string port;
        
        // You can have an instance of the client stub
        // as a member variable.
        std::unique_ptr<SNSService::Stub> stub_;
};

int main(int argc, char** argv) {

    std::string hostname = "localhost";
    std::string username = "default";
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:p:")) != -1){
        switch(opt) {
            case 'h':
                hostname = optarg;break;
            case 'u':
                username = optarg;break;
            case 'p':
                port = optarg;break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    // You MUST invoke "run_client" function to start business logic
    Client myc(hostname, username, port);
    myc.run_client();

    return 0;
}

int Client::connectTo() {
	// ------------------------------------------------------------
    // In this function, you are supposed to create a stub so that
    // you call service methods in the processCommand/porcessTimeline
    // functions. That is, the stub should be accessible when you want
    // to call any service methods in those functions.
    // I recommend you to have the stub as
    // a member variable in your own Client class.
    // Please refer to gRpc tutorial how to create a stub.
	// ------------------------------------------------------------

    std::string addr = hostname + ":" + port;
    // error if hostname and port are empty
    if (addr == ":") {
        return -1;
    }

    // Create a stub
    // 1. create a channel
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());

    // 2. pass channel into NewStub
    stub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(channel));

    // Direct client to Login
    Request req;
    Reply reply;
    ClientContext ctx;
    req.set_username(username);

    Status status = stub_->Login(&ctx, req, &reply);

    // fail if server isn't running or connection failure
    if (reply.msg() == "") {
        return -1;
    }

    std::cout << "Status Result: " + reply.msg() << "\n";

    IReply ireply;
    ireply.grpc_status = status;

    // Check if username is taken
    if (reply.msg() == "Login failed - duplicate username") {
        return 1; // TODO
    }

    return 1; // return 1 if success, otherwise return -1
}

IReply Client::processCommand(std::string& input) {
	// ------------------------------------------------------------
	// GUIDE 1:
	// In this function, you are supposed to parse the given input
    // command and create your own message so that you call an 
    // appropriate service method. The input command will be one
    // of the followings:
	//
	// FOLLOW <username>
	// UNFOLLOW <username>
	// LIST
    // TIMELINE
	//
	// ------------------------------------------------------------
	
    // ------------------------------------------------------------
	// GUIDE 2:
	// Then, you should create a variable of IReply structure
	// provided by the client.h and initialize it according to
	// the result. Finally you can finish this function by returning
    // the IReply.
	// ------------------------------------------------------------
    
	// ------------------------------------------------------------
    // HINT: How to set the IReply?
    // Suppose you have "Follow" service method for FOLLOW command,
    // IReply can be set as follow:
    // 
    //     // some codes for creating/initializing parameters for
    //     // service method
    //     IReply ire;
    //     grpc::Status status = stub_->Follow(&context, /* some parameters */);
    //     ire.grpc_status = status;
    //     if (status.ok()) {
    //         ire.comm_status = SUCCESS;
    //     } else {
    //         ire.comm_status = FAILURE_NOT_EXISTS;
    //     }
    //      
    //      return ire;
    // 
    // IMPORTANT: 
    // For the command "LIST", you should set both "all_users" and 
    // "following_users" member variable of IReply.
    // ------------------------------------------------------------
    
    IReply ireply;
    std::string command = "";
    std::string arg = "";

    ClientContext ctx;
    Request request;
    Reply reply;

    std::size_t index = input.find_first_of(" ");
    if (index != std::string::npos) {
        command = input.substr(0, index);
        arg = input.substr(index+1);

        request.set_username(username);
        request.add_arguments(arg);

        if (command.compare("FOLLOW") == 0) {
            Status status = stub_->Follow(&ctx, request, &reply);
            ireply.grpc_status = status;

            // Invalid user
            if (reply.msg() == "Follow failed - invalid user") {
                ireply.comm_status = FAILURE_INVALID_USERNAME;
            }
            // Already following
            else if (reply.msg() == "Follow failed - already following") {
                ireply.comm_status = FAILURE_ALREADY_EXISTS;
            }
            // Self follow
            else if (reply.msg() == "Follow failed - self follow") {
                ireply.comm_status = FAILURE_ALREADY_EXISTS;
            }
            // Success
            else if (reply.msg() == "Follow successful") {
                ireply.comm_status = SUCCESS;
            }
            else {
                ireply.comm_status = FAILURE_UNKNOWN;
            }
        }
        else if (command.compare("UNFOLLOW") == 0) {
            Status status = stub_->UnFollow(&ctx, request, &reply);
            ireply.grpc_status = status;

            // Invalid user
            if (reply.msg() == "Unfollow failed - invalid user") {
                ireply.comm_status = FAILURE_INVALID_USERNAME;
            }

            // Not following
            else if (reply.msg() == "Unfollow failed - not following") {
                ireply.comm_status = FAILURE_NOT_EXISTS;
            }

            // Success
            else if (reply.msg() == "Unfollow successful") {
                ireply.comm_status = SUCCESS;
            }
            else {
                ireply.comm_status = FAILURE_UNKNOWN;
            }
        }
    }
    else {
        request.set_username(username);
        
        if (input.compare("LIST") == 0) {
            Status status = stub_->List(&ctx, request, &reply);
            ireply.grpc_status = status;

            // Add users to ireply
            for (std::string uname : reply.all_users()) {
                ireply.all_users.push_back(uname);
            }
            
            // Add following to ireply
            for (std::string uname : reply.following_users()) {
                ireply.following_users.push_back(uname);
            }

            ireply.comm_status = SUCCESS;

        }
        else if (input.compare("TIMELINE") == 0) {
            ireply.comm_status = SUCCESS;
        }
    }

    return ireply;
}

void Client::processTimeline() {
	// ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // for both getting and displaying messages in timeline mode.
    // You should use them as you did in hw1.
	// ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
	// ------------------------------------------------------------

    ClientContext ctx;
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&ctx));

    Message message;

    // Send message signalling initial setup
    message.set_username(username);
    message.set_msg("INIT");
    stream->Write(message);

    // Read
    std::thread reader ([&] {
        Message msg;
        while (stream->Read(&msg)) {
            Timestamp timestamp = msg.timestamp();
            time_t time = timestamp.seconds();
            displayPostMessage(msg.username(), msg.msg(), time);
        }
    });
    reader.detach();

    std::string post;
    while(true) {

        // Get stdin
        post = getPostMessage();

        // Create message
        Message msg;
        msg.set_username(username);
        msg.set_msg(post);
        Timestamp* timestamp = new Timestamp();
        timestamp->set_seconds(time(NULL));
        timestamp->set_nanos(0);
        msg.set_allocated_timestamp(timestamp);

        // Send to server
        stream->Write(msg);
    }
    stream->WritesDone();

}
