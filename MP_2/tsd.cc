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
#include "json.hpp"

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
using json = nlohmann::ordered_json;

// Stores all data regarding users
struct User {
  std::string username;
  std::vector<User*> followers;
  std::vector<User*> following;
  ServerReaderWriter<Message, Message>* stream = 0;
};

// Local database of all clients
std::vector<User*> user_db;

int find_following(User* user, std::string following_username) {
  for (int i = 0; i < user->following.size(); i++) {
    if (user->following[i]->username == following_username) {
      return i;
    }
  }
  return -1;
}

int find_user(std::string username) {
  for (int i = 0; i < user_db.size(); i++) {
    User* u = user_db[i];
    if (u->username == username) {
      return i;
    }
  }
  return -1;
}

void UpdateJSON(json j) {
  std::ofstream ofs("data.json");
  ofs << std::setw(4) << j << std::endl;
  ofs.close();
}

// Load inital data - assumes empty local db
void LoadInitialData() {
  std::ifstream file("data.json");
  json j; 
  
  if (file.is_open()) {

    // Check if file is empty
    if (file.peek() == std::ifstream::traits_type::eof()) {
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
    
    for (auto user_data : j["users"]) {
      // Find user in local db
      User* user;
      std::string uname = user_data["username"];
      int user_index = find_user(uname);

      // Create the user if not found
      if (user_index == -1) {
        user = new User;
        user->username = uname;
      }
      // Grab the user in the local database
      else {
        user = user_db[user_index];
      }

      // Load followings / followers
      for (auto following_data : user_data["following"]) {
        std::string follow_username = following_data["username"];
        std::cout << uname << " -> " << follow_username << "... ";
        bool exit = false;

        // Find user_to_follow in local db
        User* user_to_follow;
        int following_index = find_user(follow_username);

        // Create the user if not found
        if (following_index == -1) {
          user_to_follow = new User;
          user_to_follow->username = follow_username;
          user_db.push_back(user_to_follow);
        }

        // Grab the user in the local database
        else {
          user_to_follow = user_db[following_index];
        }

        // Check if already following
        for (User* u : user->following) {
          if (u->username == user_to_follow->username) {
            std::cout << "already following\n";
            exit = true;
            break;
          }
        }

        if (exit) {
          continue;
        }

        std::cout << "added\n";
        user->following.push_back(user_to_follow);
        user_to_follow->followers.push_back(user);
      }
      
      // Add user to db
      if (user_index == -1) {
        user_db.push_back(user);
      }
    }
  }

  // Create data.json if it doesn't exist
  else {
    j = json::object();
    j["users"] = json::object();
    j["posts"] = json::array();
    UpdateJSON(j);
  }
}

// Add a new user to data.json
void CreateUserJSON(std::string username) {
  // std::cout << "adding " << username << " to storage\n";
  
  // Load data.json
  std::ifstream file("data.json");
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
  std::ifstream file("data.json");
  json j = json::parse(file);
  file.close();
  
  // TODO
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
  std::ifstream file("data.json");
  json j = json::parse(file);
  file.close();

  json post = json::object();
  post["message"] = message.msg();
  post["username"] = message.username();
  post["timestamp"] = timestamp->seconds();
  j["posts"].push_back(post);

  UpdateJSON(j);
}

class SNSServiceImpl final : public SNSService::Service {

  Status List(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------

    int user_index = find_user(request->username());
    User* user = user_db[user_index];

    // Add all users
    for (User* u : user_db) {
      reply->add_all_users(u->username);
    }

    // Add self to follows
    reply->add_following_users(request->username());

    // Add follows
    for (User* u : user->following) {
      // std::cout << user->username << " -> " << u->username << "\n";
      reply->add_following_users(u->username);
    }

    // for (User* u : user->followers) {
    //   std::cout << user->username << " <- " << u->username << "\n";
    // } 

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

    // Prevent self follow
    if (uname.compare(username_to_follow) == 0) {
      std::cout << "Follow failed - self follow\n";
      reply->set_msg("Follow failed - invalid user");
    }

    // Did not find user in database - return invalid user
    else if (follow_index == -1) {
      std::cout << "Follow failed - invalid user\n";
      reply->set_msg("Follow failed - invalid user");
    }

    // User is in database - attempt to follow
    else {
      User* user = user_db[user_index];
      User* user_to_follow = user_db[follow_index];

      // Check if user_to_follow is already followed by user
      for (User* u : user->following)  {
        if (u->username == user_to_follow->username) {
          std::cout << "Follow failed - already following\n";
          reply->set_msg("Follow failed - already following");
          return Status::OK;
        }
      }

      user->following.push_back(user_to_follow);
      user_to_follow->followers.push_back(user);

      std::cout << "Follow successful\n";
      reply->set_msg("Follow successful");

      // TODO write to json - update following
      FollowUserJSON(user->username, user_to_follow->username);
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // request from a user to unfollow one of his/her existing
    // followers
    // ------------------------------------------------------------
    std::cout << "Unfollow attempted - " << request->username() << "... ";

    std::string uname = request->username();
    std::string username_to_unfollow = request->arguments(0);
    int user_index = find_user(uname);
    int unfollow_index = find_user(username_to_unfollow);

    // Prevent self unfollow
    if (uname.compare(username_to_unfollow) == 0) {
      std::cout << "Unfollow failed - self unfollow\n";
      reply->set_msg("Unfollow failed - invalid user");
    }

    // Did not find user in database - return invalid user 
    else if (unfollow_index == -1) {
      std::cout << "Unfollow failed - invalid user\n";
      reply->set_msg("Unfollow failed - invalid user");
    }

    // User is in following list - attempt to unfollow
    else {
      bool unfollowing = false;
      bool unfollowers = false;

      // Undo user->following.push_back(*user_to_follow);
      std::vector<User*>* following_list = &(user_db[user_index]->following);
      for (int i = 0; i < following_list->size(); i++) {
        if (following_list->at(i)->username == username_to_unfollow) {
          following_list->erase(following_list->begin() + i);
          unfollowing = true;
          break;
        }
      }

      // Undo user_to_follow->followers.push_back(*user);
      std::vector<User*>* followers_list = &(user_db[unfollow_index]->followers);
      for (int i = 0; i < followers_list->size(); i++) {
        if (followers_list->at(i)->username == uname) {
          followers_list->erase(followers_list->begin() + i);
          unfollowers = true;
          break;
        }
      }

      if (unfollowing && unfollowers) {
        std::cout << "Unfollow successful\n";
        reply->set_msg("Unfollow successful");
      }
      else {
        std::cout << "Unfollow failed - not following\n";
        reply->set_msg("Unfollow failed - not following");
      }

    }

    // TODO - write to file

    return Status::OK;
  }
  
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles 
    // a new user and verify if the username is available
    // or already taken
    // ------------------------------------------------------------

    std::cout << "Login attempted - " << request->username() << "... ";

    User* user;
    std::string uname = request->username();
    int user_index = find_user(uname);

    // No user with the username found -- add them into the database and let them login
    if (user_index == -1) {
      user = new User;
      user->username = uname;
      user_db.push_back(user);

      // TODO - write to json - create a user with username, following, and posts fields
      std::cout << "Login successful\n";
      CreateUserJSON(uname);

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
    std::cout << "Timeline activated - ";
    Message message_recv;
    Message message_send;
    std::string uname;
    int user_index = -1;
    bool init = true;

    while (stream->Read(&message_recv)) {
      User* user;

      // Check if inital setup
      if (message_recv.msg() == "INIT" && init) {

        init = false;
        uname = message_recv.username();
        std::cout << uname << "\n";
        user_index = find_user(uname);
        user = user_db[user_index];

        if (user->stream == 0) {
          user->stream = stream;
        }

        // Retrieve following messages - up to 20
        int count = 0;

        // Load json
        std::ifstream file("data.json");
        json j = json::parse(file);
        file.close();

        // Get the recent 20 messages
        
        json posts = j["posts"];
        for (auto it = posts.rbegin(); it != posts.rend(); it++) {
          if (count >= 20) {
            break;
          }

          std::string message_username = (*it)["username"];
          if (uname != message_username) {
            // Check if username is followed
            if (find_following(user, message_username) < 0) {
              continue;
            }

            // Check if message is after follow age
            int64_t follow_age = j["users"][uname]["following"][message_username]["timestamp"];
            int64_t message_seconds = (*it)["timestamp"];

            if (follow_age > message_seconds) {
              continue;
            }
          }


          // Create message
          message_send.set_username((*it)["username"]);
          message_send.set_msg((*it)["message"]);
          Timestamp* timestamp = new Timestamp();
          timestamp->set_seconds((*it)["timestamp"]);
          timestamp->set_nanos(0);
          message_send.set_allocated_timestamp(timestamp);

          // Send to client
          stream->Write(message_send);
          count++;
        }
        
      }

      // Send post to followers
      else {
        std::string str = message_recv.msg();

        // Create message
        message_send.set_username(uname);
        message_send.set_msg(str);
        Timestamp* timestamp = new Timestamp();
        timestamp->set_seconds(time(NULL));
        timestamp->set_nanos(0);
        message_send.set_allocated_timestamp(timestamp);

        // send post to followers
        for (User* u : user->followers) {
          if (u->stream != 0) {
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

  // load inital data into local user_db
  LoadInitialData();

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
