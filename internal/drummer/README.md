# About Drummer #

Drummer is an __optional__ component. It is provided as the reference implementation of the Master server for managing large number of distributed NodeHost instances - 
* Details about Raft clusters reported via NodeHost's IMasterClient are stored in the DrummerDB and replicated across multiple Drummer servers.
* NodeHost and its managed Raft nodes are considered as dead when it fails to contact Drummer servers for certain amount of time.
* A Drummer server will be elected as the leader, it monitors the the availability of known NodeHost instances and associated Raft clusters, new Raft nodes will be spawned whenever necessary to replace failed ones.
* User applications can query Drummer servers to locate interested Raft clusters. Raft clusters can then be accessed via the NodeHost API interface.

Drummer is also used in Dragonboat's monkey testing to guard the correctness of our Raft implementation.

## When to use Master servers ##
Do not use Master servers when - 
* you have a small and simple deployment
* you want to use your own facilities for node failure detection and handling
* you want to have full control of the system

Use Master servers when -
* you have large number of NodeHost instances
* you want to delegate node failure detection and handling to a dedicated component known as Master servers

## Resources ##

[dragonboat-drummer-cmd](server/drummercmd/README.md) is a simple command line tool used to interact with Drummer. 

## Deployment ##
There are two major phases involved to deploy and operate a Dragonboat based application using Drummer.

First is the launch phase, dragonboat-drummer-cmd can be used to define what Raft clusters are required and how many replicas in each cluster. Once all required Raft clusters are defines, they can be launched. Drummer will select suitable NodeHosts and spawn Raft nodes on selected NodeHosts. This launch process typically takes less than one minute and NodeHosts are expected not to crash during the launch phase. 

Once the above launch phase is successfully completed, the system enters the operation phase. Drummer then starts to monitor all connected NodeHosts and repair failed Raft nodes by spawning new ones on other available NodeHosts when necessary. Users can add more NodeHosts to expand resources available to Drummer or to replace failed NodeHost servers. The dragonboat-drummer-cmd tool can be used to query NodeHost and Raft cluster status.

## Requirements ##
Drummer depends on Go [gRPC](https://github.com/grpc/grpc-go). To get Go gRPC -
```
$ go get -u google.golang.org/grpc
```
If you are behind the GFW and always timeout on golang.org/x packages, there are [easy ways](https://github.com/lni/dragonboat/issues/2?ts=2) to workaround.
