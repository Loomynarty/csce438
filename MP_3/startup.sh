#!/bin/bash

# Kill all processes if script is interrupted
function kill_processes {
    echo -e "\nRunning kill script"
    killall coordinator
    killall server
    killall followsync
}

trap kill_processes SIGINT SIGTERM

# Start coordinator process
./coordinator &

# Start master server processes
./server -p 8010 -i 1 -t master &
./server -p 8020 -i 2 -t master &
./server -p 8030 -i 3 -t master &

# Start slave server processes
./server -p 8040 -i 1 -t slave &
./server -p 8050 -i 2 -t slave &
./server -p 8060 -i 3 -t slave &

# Start followsync processes
./followsync -p 8070 -i 1 &
./followsync -p 8080 -i 2 &
./followsync -p 8090 -i 3 &

# Wait for all processes to finish
echo "Waiting"
wait


