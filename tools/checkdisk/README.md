## Checkdisk ##

Checkdisk is simple program used to show relative performance when using different types of SSDs. There are many minor changes that can be made to make it faster, but this is _NOT_ a benchmark program so making it faster is never the goal.

### How it works ###

This program creats 48 Raft groups each with only one node. It then uses 10,000 client goroutines to keep making proposals with 16 bytes payloads on those 48 Raft groups. It reports the average number of completed proposals per second after keep making proposals for 60 seconds.

To build the program - 
```
go build github.com/lni/dragonboat/v4/tools/checkdisk
```

An executable file named checkdisk will be generated in the current directory. Copy it to the disk you want to check and run the executable. It takes roughly 1 minute to complete. 

### Results ###

Results below were observed on a single socket E5-2696v4 server (22 cores) running Linux kernel 5.0 with ext4 filesystem.

|Brand|Model|Type|Num of proposals per second|
|:---:|:---:|:--:|:-------------------------:|
|Intel|P4510 2T|MVME|7210419|
|Intel|900P 280G|NVME, Optane|6772066|
|Intel|P3700 1.6T|NVME|6896307|
|Intel|P3520 1.2T|NVME|6465987|
|Intel|S3710 800G|SATA|3094821|
|Intel|S4510 960G|SATA|3082143|
|Samsung|860EVO 500G|SATA|922439|
|Samsung|850EVO 500G|SATA|916712|
