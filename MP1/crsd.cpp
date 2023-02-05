#include <glog/logging.h>
#include "interface.h"
// TODO: Implement Chat Server.

int main(int argc, char *argv[]){
    // Change log location to a dedicated folder
    FLAGS_log_dir = "./logs/";
    // Also log to the terminal
    FLAGS_alsologtostderr = 1;
    google::InitGoogleLogging(argv[0]);

    LOG(INFO) << "Starting Server";

    while(true) {

    }
}

