# DevOps #

This document describes the DevOps requirements for operating Dragonboat based applications in production. Please note that incorrect DevOps operations can potentially corrupt your Raft shards permanently.

* It is recommended to use the ext4 filesystem, other filesystems have never been tested.
* It is recommended to use enterprise NVME SSD with high write endurance rating. Must use local hard disks and avoid any NFS, CIFS, Samba, CEPH or other similar shared storage.
* Never try to backup or restore Dragonboat data by directly operating on Dragonboat data files or directories. It can immediately corrupt your Raft shards. 
* Each Raft group has multiple replicas, the best way to safeguard the availability of your services and data is to increase the number of replicas. As an example, the Raft group can tolerant 2 node failures when there are 5 replicas, while it can only tolerant 1 node failure when using 3 replicas. 
* On node failure, the Raft group will be available when it still has the quorum. To handle such failures, you can add a non-voting node to start replicating data to it, once in sync with other replicas you can promote the non-voting node to a regular node and remove the failed node by using membership change APIs. For those failed nodes caused by intermittent failures such as short term network partition or power loss, you should resolve the network or power issue and try restarting the affected nodes.
* On disk failure, such as when experiencing data integrity check errors or write failures, it is important to immediately replace the failed disk and remove the failed node using the above described membership change method. To restart nodes with such disk failures, it is important to have the failed disk replaced first to ensure corrupted data is removed. As a refreshed node with no existing data, that node must be assigned a new RaftAddress value to avoid confusing other nodes. 
* When the quorum nodes are gone, you will not be able to resolve it without losing data. The github.com/lni/dragonboat/tools package provides the ImportSnapshot method to import a previously exported snapshot to repair such failed Raft shard.
* By default, the RaftAddress value can not be changed between restarts, otherwise the system will panic with an error message.
* When you can't provide a static IP for your nodes, e.g. when IP is dynamically assigned on node restart, you may want to configure a static DNS name for that node and update it on each restart. 
* When it is not possible to do so, you can choose to set the DefaultNodeRegistryEnabled field to enable the gossip feature which is designed to handle dynamic RaftAddress. Check godocs for more details on the gossip feature. 
* Always test your system to ensure that it has high availability by design, disaster recovery should always be a part of the CI.
