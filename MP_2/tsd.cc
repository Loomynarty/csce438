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

#include "sns.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

// Stores all data regarding users
struct User {
  std::string username;
  std::vector<User> followers;
  std::vector<User> following;
  ServerReaderWriter<Message, Message>* stream = 0;
};

// Local database of all clients
std::vector<User> user_db;

class SNSServiceImpl final : public SNSService::Service {

  private:
    int find_user(std::string username) {
      for (int i = 0; i < user_db.size(); i++) {
        User u = user_db[i];
        if (u.username == username) {
          return i;
        }
      }
      return -1;
    }

  Status List(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // request from a user to follow one of the existing
    // users
    // ------------------------------------------------------------
    std::cout << "Follow attempted - " << request->username() << "... ";

    std::string uname = request->username();
    std::string username_to_follow = request->arguments(0);
    int user_index = find_user(uname);
    int follow_index = find_user(username_to_follow);

    // Did not find user in database - return invalid user
    if (follow_index == -1) {
      std::cout << "Follow failed - invalid user\n";
      reply->set_msg("Follow failed - invalid user");
    }
    // User is in database - attempt to follow
    else {
      User* user = &user_db[user_index];
      User* user_to_follow = &user_db[follow_index];

      // Check if user_to_follow is already followed by user
      for (User u : user->following)  {
        if (u.username == user_to_follow->username) {
          std::cout << "Follow failed - already following\n";
          reply->set_msg("Follow failed - already following");
          return Status::OK;
        }
      }

      user->following.push_back(*user_to_follow);
      user_to_follow->followers.push_back(*user);

      // TODO - write to file

      std::cout << "Follow successful\n";
      reply->set_msg("Follow successful");
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // request from a user to unfollow one of his/her existing
    // followers
    // ------------------------------------------------------------
    return Status::OK;
  }
  
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // a new user and verify if the username is available
    // or already taken
    // ------------------------------------------------------------

    std::cout << "Login attempted - " << request->username() << "... ";

    User user;
    std::string uname = request->username();
    int user_index = find_user(uname);

    // No user with the username found -- add them into the database and let them login
    if (user_index == -1) {
      user.username = uname;
      user_db.push_back(user);

      // TODO - write to file

      std::cout << "Login successful\n";
      reply->set_msg("Login successful");
    }

    // Username found -- prevent login
    else {
      std::cout << "Login failed\n";
      reply->set_msg("Login failed - duplicate username");
    }

    
    return Status::OK;
  }

  Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // receiving a message/post from a user, recording it in a file
    // and then making it available on his/her follower's streams
    // ------------------------------------------------------------
    return Status::OK;
  }

};

void RunServer(std::string port_no) {
  // ------------------------------------------------------------
  // In this function, you are to write code 
  // which would start the server, make it listen on a particular
  // port number.
  // ------------------------------------------------------------
  std::string server_addr = "localhost:" + port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_addr + "\n";

  // TODO - load file

  server->Wait();

}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}
