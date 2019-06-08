## checkdisk ##

Checkdisk shows rough disk performance in dragonboat.

There are many minor changes that can be made to make it faster, but this is _NOT_ a benchmark program. It is a simple program to show relative performance when using different types of SSDs.

### How it works ###

This program creats 48 Raft groups each with only one node. It then uses 10,000 client goroutines to keep making proposals with 16 bytes payload on those 48 Raft groups. It reports the average number of completed proposals per second after keep making proposals for 60 seconds.

### Results ###

Results below were observed on a single socket E5-2696v4 server with 22 cores.

|Brand|Model|Type|Num of proposals per second|
|Intel|900P 280G|NVME, Optane|6772066|
|Intel|P3700 1.6T|NVME|6896307|
|Intel|P3520 1.2T|NVME|6465987|
|Intel|S3710 800G|SATA|3094821|
|Intel|S4510 960G|SATA||
|Samsung|860EVO 500G|SATA|922439|
