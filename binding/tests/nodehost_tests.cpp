// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <condition_variable>
#include <sys/stat.h>

#include "zupply.h"
#include "dragonboat/dragonboat.h"
#include "gtest/gtest.h"
#include "dragonboat/statemachine/regular.h"
#include "dragonboat/statemachine/concurrent.h"
#include "dragonboat/statemachine/ondisk.h"

// State Machines

class TestRegularStateMachine : public dragonboat::RegularStateMachine {
 public:
  TestRegularStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept
    : dragonboat::RegularStateMachine(clusterID, nodeID), count_(0)
  {}

  ~TestRegularStateMachine()
  {}
 protected:
  void update(dragonboat::Entry &ent) noexcept override
  {
    count_++;
    ent.result = count_;
  }

  LookupResult lookup(
    const dragonboat::Byte *data,
    size_t size) const noexcept override
  {
    LookupResult r;
    r.result = new char[sizeof(uint64_t)];
    r.size = sizeof(uint64_t);
    *((uint64_t *) r.result) = count_;
    return r;
  }

  uint64_t getHash() const noexcept override
  {
    return count_;
  }

  SnapshotResult saveSnapshot(
    dragonboat::SnapshotWriter *writer,
    dragonboat::SnapshotFileCollection *collection,
    const dragonboat::DoneChan &done) const noexcept override
  {
    SnapshotResult r;
    dragonboat::IOResult ret;
    r.errcode = SNAPSHOT_OK;
    r.size = 0;
    ret = writer->Write((dragonboat::Byte *) &count_, sizeof(uint64_t));
    if (ret.size != sizeof(uint64_t)) {
      r.errcode = FAILED_TO_SAVE_SNAPSHOT;
      return r;
    }
    r.size = sizeof(uint64_t);
    return r;
  }

  int recoverFromSnapshot(
    dragonboat::SnapshotReader *reader,
    const std::vector<dragonboat::SnapshotFile> &files,
    const dragonboat::DoneChan &done) noexcept override
  {
    dragonboat::IOResult ret;
    dragonboat::Byte data[sizeof(uint64_t)];
    ret = reader->Read(data, sizeof(uint64_t));
    if (ret.size != sizeof(uint64_t)) {
      return FAILED_TO_RECOVER_FROM_SNAPSHOT;
    }
    count_ = (uint64_t) (*data);
    return SNAPSHOT_OK;
  }

  void freeLookupResult(LookupResult r) noexcept override
  {
    delete[] r.result;
  }
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(TestRegularStateMachine);
  uint64_t count_;
};

class TestConcurrentStateMachine : public dragonboat::ConcurrentStateMachine {
 public:
  TestConcurrentStateMachine(uint64_t clusterID, uint64_t nodeID) noexcept
    : dragonboat::ConcurrentStateMachine(clusterID, nodeID),
      count_(0)
  {}

  ~TestConcurrentStateMachine()
  {}
 protected:
  void batchedUpdate(std::vector<dragonboat::Entry> &ents) noexcept override
  {
    for (auto &it : ents) {
      count_++;
      it.result = count_;
    }
  }

  LookupResult lookup(const dragonboat::Byte *data,
    size_t size) const noexcept override
  {
    LookupResult r;
    r.result = new char[sizeof(uint64_t)];
    r.size = sizeof(uint64_t);
    *((uint64_t *) r.result) = count_;
    return r;
  }

  uint64_t getHash() const noexcept override
  {
    return count_;
  }

  PrepareSnapshotResult prepareSnapshot() const noexcept override
  {
    PrepareSnapshotResult r;
    r.result = new char[sizeof(uint64_t)];
    r.errcode = 0;
    memcpy(r.result, &count_, sizeof(uint64_t));
    return r;
  }

  SnapshotResult saveSnapshot(
    const void *context,
    dragonboat::SnapshotWriter *writer,
    dragonboat::SnapshotFileCollection *collection,
    const dragonboat::DoneChan &done) const noexcept override
  {
    auto ret = writer->Write((const dragonboat::Byte *)context, 8);
    SnapshotResult r;
    r.errcode = SNAPSHOT_OK;
    r.size = ret.size;
    if(ret.size != 8 || ret.error != 0) {
      r.errcode = FAILED_TO_SAVE_SNAPSHOT;
    }
    delete[] (char *)context;
    return r;
  }

  int recoverFromSnapshot(
    dragonboat::SnapshotReader *reader,
    const std::vector<dragonboat::SnapshotFile> &files,
    const dragonboat::DoneChan &done) noexcept override
  {
    dragonboat::IOResult ret;
    dragonboat::Byte data[sizeof(uint64_t)];
    ret = reader->Read(data, sizeof(uint64_t));
    if (ret.size != sizeof(uint64_t)) {
      return FAILED_TO_RECOVER_FROM_SNAPSHOT;
    }
    count_ = *(uint64_t*)data;
    return SNAPSHOT_OK;
  }

  void freeLookupResult(LookupResult r) noexcept override
  {
    delete[] r.result;
  }
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(TestConcurrentStateMachine);
  uint64_t count_;
};

class TestOnDiskStateMachine : public dragonboat::OnDiskStateMachine {
 public:
  TestOnDiskStateMachine(uint64_t clusterID, uint64_t nodeID,
    uint64_t initialApplied) noexcept
    : dragonboat::OnDiskStateMachine(clusterID, nodeID),
      initialApplied_(initialApplied),
      count_(0),
      index_(nullptr)
  {}

  TestOnDiskStateMachine(uint64_t clusterID, uint64_t nodeID,
    uint64_t *index) noexcept
    : dragonboat::OnDiskStateMachine(clusterID, nodeID),
      initialApplied_(0),
      count_(0),
      index_(index)
  {}

  ~TestOnDiskStateMachine()
  {}
 protected:
  OpenResult open(const dragonboat::DoneChan &done) noexcept override
  {
    OpenResult r;
    r.result = initialApplied_;
    r.errcode = OPEN_OK;
    return r;
  }

  void batchedUpdate(std::vector<dragonboat::Entry> &ents) noexcept override
  {
    for (auto &it : ents) {
      count_++;
      it.result = count_;
      if (index_) {
        *index_ = it.index;
      }
    }
  }

  LookupResult lookup(const dragonboat::Byte *data,
    size_t size) const noexcept override
  {
    LookupResult r;
    r.result = new char[sizeof(uint64_t)];
    r.size = sizeof(uint64_t);
    *((uint64_t *) r.result) = count_;
    return r;
  }

  int sync() const noexcept override
  {
    return SYNC_OK;
  }

  uint64_t getHash() const noexcept override
  {
    return count_;
  }

  PrepareSnapshotResult prepareSnapshot() const noexcept override
  {
    PrepareSnapshotResult r;
    r.result = new char[2*sizeof(uint64_t)];
    r.errcode = 0;
    memcpy(r.result, &initialApplied_, sizeof(uint64_t));
    memcpy((char *)r.result + sizeof(uint64_t), &count_, sizeof(uint64_t));
    return r;
  }

  SnapshotResult saveSnapshot(
    const void *context,
    dragonboat::SnapshotWriter *writer,
    const dragonboat::DoneChan &done) const noexcept override
  {
    auto ret = writer->Write((const dragonboat::Byte *)context, 16);
    SnapshotResult r;
    r.errcode = SNAPSHOT_OK;
    r.size = ret.size;
    if(ret.size != 16 || ret.error != 0) {
      r.errcode = FAILED_TO_SAVE_SNAPSHOT;
    }
    delete[] (char *)context;
    return r;
  }

  int recoverFromSnapshot(
    dragonboat::SnapshotReader *reader,
    const dragonboat::DoneChan &done) noexcept override
  {
    dragonboat::IOResult ret;
    dragonboat::Byte data[2*sizeof(uint64_t)];
    ret = reader->Read(data, 2*sizeof(uint64_t));
    if (ret.size != 2*sizeof(uint64_t)) {
      return FAILED_TO_RECOVER_FROM_SNAPSHOT;
    }
    initialApplied_ = *(uint64_t*)data;
    count_ = *(uint64_t*)(data + sizeof(uint64_t));
    return SNAPSHOT_OK;
  }

  void freeLookupResult(LookupResult r) noexcept override
  {
    delete[] r.result;
  }
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(TestOnDiskStateMachine);
  uint64_t initialApplied_;
  uint64_t count_;
  uint64_t *index_;
};

// State Machine Factories

dragonboat::RegularStateMachine *CreateRegularStateMachine(
  uint64_t clusterID,
  uint64_t nodeID)
{
  return new TestRegularStateMachine(clusterID, nodeID);
}

dragonboat::RegularStateMachine *ExtraCreateRegularStateMachine(
  uint64_t clusterID,
  uint64_t nodeID,
  uint64_t placeHolder)
{
  return new TestRegularStateMachine(clusterID, nodeID);
}

dragonboat::ConcurrentStateMachine *CreateConcurrentStateMachine(
  uint64_t clusterID,
  uint64_t nodeID)
{
  return new TestConcurrentStateMachine(clusterID, nodeID);
}

dragonboat::OnDiskStateMachine *CreateOnDiskStateMachine(
  uint64_t clusterID,
  uint64_t nodeID)
{
  return new TestOnDiskStateMachine(clusterID, nodeID, uint64_t(0));
}

// Event for asynchronous methods

class TestEvent : public dragonboat::Event {
 public:
  TestEvent() noexcept
    : set_(false)
  {}
  void Wait() noexcept
  {
    std::unique_lock<std::mutex> lk(m_);
    while (!set_) {
      cv_.wait(
        lk, [this]()
        { return set_; });
    }
  }
 protected:
  void set() noexcept
  {
    std::lock_guard<std::mutex> lk(m_);
    set_ = true;
    cv_.notify_one();
  }
 private:
  bool set_;
  std::condition_variable cv_;
  std::mutex m_;
};

// Global Configuration

class NodeHostTest : public ::testing::Test {
 protected:
  virtual void SetUp();
  virtual void TearDown();
  bool TwoNodeHostRequired();
  bool EventListenerRequired();

  dragonboat::NodeHostConfig getTestNodeHostConfig();
  dragonboat::Config getTestConfig();
  void waitForElectionToComplete(bool);
  bool snapshotExist(const std::string &dir);

  const static std::string NodeHostTestDir;
  const static std::string NodeHostTestDir2;
  const static std::string ExportedSnapshotDir;
  const static std::string RaftAddress;
  const static std::string RaftAddress2;
  const static std::string TestPluginFilename;
  std::unique_ptr<dragonboat::NodeHost> nh_;
  std::unique_ptr<dragonboat::NodeHost> nh2_;
  uint64_t gi_oid_;
  uint64_t managed_object_count_;
  std::atomic_bool flag_;
};

const std::string
  NodeHostTest::NodeHostTestDir = "nodehost_test_dir_safe_to_delete";
const std::string
  NodeHostTest::NodeHostTestDir2 = "nodehost_test_dir2_safe_to_delete";
const std::string
  NodeHostTest::ExportedSnapshotDir = "nodehost_test_snapshotdir_safe_to_delete";
const std::string
  NodeHostTest::TestPluginFilename = "dragonboat-cpp-plugin-example.so";
const std::string
  NodeHostTest::RaftAddress = "localhost:9050";
const std::string
  NodeHostTest::RaftAddress2 = "localhost:9051";

dragonboat::NodeHostConfig NodeHostTest::getTestNodeHostConfig()
{
  dragonboat::NodeHostConfig nhConfig(NodeHostTestDir, NodeHostTestDir);
  nhConfig.DeploymentID = 1;
  nhConfig.RTTMillisecond = dragonboat::Milliseconds(20);
  nhConfig.RaftAddress = RaftAddress;
  nhConfig.MutualTLS = true;
  nhConfig.CAFile = "internal/transport/tests/test-root-ca.crt";
  nhConfig.CertFile = "internal/transport/tests/localhost.crt";
  nhConfig.KeyFile = "internal/transport/tests/localhost.key";
  return nhConfig;
}

dragonboat::Config NodeHostTest::getTestConfig()
{
  dragonboat::Config config(1, 1);
  config.CheckQuorum = false;
  config.Quiesce = false;
  config.ElectionRTT = 5;
  config.HeartbeatRTT = 1;
  config.SnapshotEntries = 20;
  config.CompactionOverhead = 20;
  return config;
}

void NodeHostTest::waitForElectionToComplete(bool useNodeHost2 = false)
{
  bool done = false;
  for (int i = 0; i < 1000; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    dragonboat::LeaderID leaderID;
    dragonboat::Status s;
    if (!useNodeHost2) {
      s = nh_->GetLeaderID(1, &leaderID);
    } else {
      s = nh2_->GetLeaderID(1, &leaderID);
    }
    if (s.OK() && leaderID.HasLeaderInfo()) {
      done = true;
      break;
    }
  }
  EXPECT_TRUE(done);
}

bool NodeHostTest::snapshotExist(const std::string &dir)
{
  auto items = zz::os::list_directory(dir);
  struct stat info;
  for (auto &item : items) {
    auto base = zz::os::path_split_basename(item);
    if (zz::os::is_file(item) && zz::fmt::starts_with(base, "snapshot")) {
        stat(item.c_str(), &info);
        std::cout << "snapshot: " << item << ", size: " << info.st_size << std::endl;
        return bool(info.st_size);
    } else if (zz::os::is_directory(item) && snapshotExist(item)) {
      return true;
    }
  }
  return false;
}

void NodeHostTest::SetUp()
{
  managed_object_count_ = CGetManagedObjectCount();
  gi_oid_ = CGetInterestedGoroutines();
  zz::fs::Path p1(NodeHostTestDir);
  if (p1.exist() && p1.is_dir()) {
    zz::os::remove_dir(NodeHostTestDir);
  }
  zz::os::create_directory_recursive(NodeHostTestDir);
  zz::fs::Path ss(ExportedSnapshotDir);
  if (ss.exist() && ss.is_dir()) {
    zz::os::remove_dir(ExportedSnapshotDir);
  }
  zz::os::create_directory_recursive(ExportedSnapshotDir);
  auto nhConfig = getTestNodeHostConfig();
  if (EventListenerRequired()) {
    flag_ = false;
    nhConfig.RaftEventListener = [this](LeaderInfo info) {
      flag_ = true;
      std::cout << "leader updated is reported: "
        << "ClusterID " << info.ClusterID
        << ", NodeID " << info.NodeID
        << ", Term " << info.Term
        << ", LeaderID " << info.LeaderID << std::endl;
    };
  }
  nh_.reset(new dragonboat::NodeHost(nhConfig));
  if (TwoNodeHostRequired()) {
    zz::fs::Path p2(NodeHostTestDir2);
    if (p2.exist() && p2.is_dir()) {
      zz::os::remove_dir(NodeHostTestDir2);
    }
    zz::os::create_directory_recursive(NodeHostTestDir2);
    dragonboat::NodeHostConfig nhConfig2(NodeHostTestDir2, NodeHostTestDir2);
    nhConfig2.RaftAddress = RaftAddress2;
    nhConfig2.DeploymentID = 1;
    nhConfig2.MutualTLS = true;
    nhConfig2.CAFile = "internal/transport/tests/test-root-ca.crt";
    nhConfig2.CertFile = "internal/transport/tests/localhost.crt";
    nhConfig2.KeyFile = "internal/transport/tests/localhost.key";
    nh2_.reset(new dragonboat::NodeHost(nhConfig2));
  }
}

void NodeHostTest::TearDown()
{
  std::cout << "tear down stop called" << std::endl;
  nh_->Stop();
  std::cout << "tear down stop returned" << std::endl;
  nh_ = nullptr;
  zz::fs::Path p1(NodeHostTestDir);
  if (p1.exist() && p1.is_dir()) {
    zz::os::remove_dir(NodeHostTestDir);
  }
  zz::fs::Path ss(ExportedSnapshotDir);
  if (ss.exist() && ss.is_dir()) {
    zz::os::remove_dir(ExportedSnapshotDir);
  }
  if (TwoNodeHostRequired()) {
    zz::fs::Path p2(NodeHostTestDir2);
    if (p2.exist() && p2.is_dir()) {
      zz::os::remove_dir(NodeHostTestDir2);
    }
    nh2_->Stop();
    nh2_ = nullptr;
  }
  CAssertNoGoroutineLeak(gi_oid_);
  uint64_t v = CGetManagedObjectCount();
  EXPECT_EQ(v, managed_object_count_);
}

bool NodeHostTest::TwoNodeHostRequired()
{
  std::string
    name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  if (name.find("ObserverCanSyncPropose") != std::string::npos ||
    name.find("ObserverCanReadIndex") != std::string::npos ||
    name.find("ObserverCanStaleRead") != std::string::npos ||
    name.find("SnapshotCanBeStreamed") != std::string::npos) {
    return true;
  }
  return false;
}

bool NodeHostTest::EventListenerRequired()
{
  std::string
    name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  if (name.find("LeaderUpdated") != std::string::npos) {
    return true;
  }
  return false;
}

// Test NodeHost

TEST_F(NodeHostTest, CanStartClusterUsingLambda)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  auto closure = 1;
  dragonboat::Status s = nh_->StartCluster(p, false,
    [&closure](uint64_t clusterID, uint64_t nodeID) {
      EXPECT_EQ(nodeID, closure);
      return CreateRegularStateMachine(clusterID, closure);
    },
    config);
  EXPECT_TRUE(s.OK());
  s = nh_->StopCluster(1);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, CanStartClusterUsingStdFunction)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  auto closure = 1;
  dragonboat::Status s = nh_->StartCluster(p, false,
    std::bind(ExtraCreateRegularStateMachine,
      std::placeholders::_1, std::placeholders::_2, closure),
    config);
  EXPECT_TRUE(s.OK());
  s = nh_->StopCluster(1);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, ClusterCanBeAddedAndRemoved)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  s = nh_->StopCluster(1);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, NodeCanBeStopped)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  s = nh_->StopNode(1, 1);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, ClusterCanNotBeAddedTwice)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrClusterAlreadyExist);
}

TEST_F(NodeHostTest, JoiningWithPeerListIsNotAllowed)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, true, CreateRegularStateMachine,
    config);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrInvalidClusterSettings);
}

TEST_F(NodeHostTest, RestartingWithDifferentPeerSetIsNotAllowed)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  dragonboat::Peers p2;
  p2.AddMember("localhost:9051", 1);
  s = nh_->StopCluster(config.ClusterId);
  EXPECT_TRUE(s.OK());
  s = nh_->StartCluster(p2, false, CreateRegularStateMachine, config);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrInvalidClusterSettings);
}

TEST_F(NodeHostTest, JoinAnInitialPeerIsNotAllowed)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  dragonboat::Peers p2;
  s = nh_->StopCluster(config.ClusterId);
  EXPECT_TRUE(s.OK());
  s = nh_->StartCluster(p2, true, CreateRegularStateMachine, config);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrInvalidClusterSettings);
}

TEST_F(NodeHostTest, RestartPreviouslyJoinedNodeWithPeerSetIsNotAllowed)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  dragonboat::Status s = nh_->StartCluster(
    p, true, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  s = nh_->StopCluster(config.ClusterId);
  EXPECT_TRUE(s.OK());
  dragonboat::Peers p2;
  p2.AddMember("localhost:9050", 1);
  s = nh_->StartCluster(p2, false, CreateRegularStateMachine, config);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrInvalidClusterSettings);
}

// TODO: lni
// add tests to check failed add cluster is reported with expected error code

TEST_F(NodeHostTest, FailedRemoveClusterIsReported)
{
  dragonboat::Status s = nh_->StopCluster(1);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrClusterNotFound);
}

TEST_F(NodeHostTest, SessionCanBeCreatedAndClosed)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  s = nh_->SyncCloseSession(*(cs.get()), timeout);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, LeaderTransferCanBeRequested)
{
  dragonboat::Status s = nh_->RequestLeaderTransfer(1, 1);
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrClusterNotFound);
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  s = nh_->RequestLeaderTransfer(1, 1);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, LeaderUpdatedIsReported)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  EXPECT_FALSE(flag_);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  for (int i = 0; i < 10; i++) {
    if (flag_) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(flag_);
}

TEST_F(NodeHostTest, LeaderIDCanBeQueried)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  dragonboat::LeaderID leaderID;
  s = nh_->GetLeaderID(1, &leaderID);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(leaderID.GetLeaderID(), 1);
  EXPECT_TRUE(leaderID.HasLeaderInfo());
  s = nh_->GetLeaderID(2, &leaderID);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrClusterNotFound);
}

TEST_F(NodeHostTest, ClusterMembershipCanBeQueried)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  auto ts = dragonboat::Milliseconds(1000);
  dragonboat::Peers rp;
  s = nh_->GetClusterMembership(1, ts, &rp);
  EXPECT_TRUE(s.OK());
  auto m = rp.GetMembership();
  EXPECT_EQ(m.size(), 1);
  auto search = m.find("localhost:9050");
  EXPECT_TRUE(search != m.end());
  EXPECT_EQ(search->second, 1);
  s = nh_->GetClusterMembership(2, ts, &rp);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrClusterNotFound);
}

TEST_F(NodeHostTest, NodeHostInfoCanBeQueried)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();

  dragonboat::NodeHostInfoOption option;
  option.SkipLogInfo = false;
  dragonboat::NodeHostInfo info = nh_->GetNodeHostInfo(option);
  EXPECT_EQ(info.ClusterInfoList.size(), 0);
  EXPECT_EQ(info.LogInfo.size(), 0);

  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();

  info = nh_->GetNodeHostInfo(option);
  EXPECT_EQ(info.ClusterInfoList.size(), 1);
  dragonboat::ClusterInfo cinfo = info.ClusterInfoList.back();
  EXPECT_EQ(cinfo.ClusterID, 1);
  EXPECT_EQ(cinfo.NodeID, 1);
  EXPECT_TRUE(cinfo.IsLeader);
  EXPECT_FALSE(cinfo.IsObserver);
  EXPECT_EQ(cinfo.SMType, REGULAR_STATEMACHINE);
  EXPECT_EQ(cinfo.Nodes.size(), 1);
  EXPECT_EQ(cinfo.Nodes[1], "localhost:9050");
  EXPECT_EQ(cinfo.ConfigChangeIndex, 1);
  EXPECT_FALSE(cinfo.Pending);
  EXPECT_EQ(info.LogInfo.size(), 1);
  dragonboat::NodeInfo linfo = info.LogInfo.back();
  EXPECT_EQ(linfo.ClusterID, 1);
  EXPECT_EQ(linfo.NodeID, 1);

  option.SkipLogInfo = true;
  info = nh_->GetNodeHostInfo(option);
  EXPECT_EQ(info.ClusterInfoList.size(), 1);
  cinfo = info.ClusterInfoList.back();
  EXPECT_EQ(cinfo.ClusterID, 1);
  EXPECT_EQ(cinfo.NodeID, 1);
  EXPECT_TRUE(cinfo.IsLeader);
  EXPECT_FALSE(cinfo.IsObserver);
  EXPECT_EQ(cinfo.SMType, REGULAR_STATEMACHINE);
  EXPECT_EQ(cinfo.Nodes.size(), 1);
  EXPECT_EQ(cinfo.Nodes[1], "localhost:9050");
  EXPECT_EQ(cinfo.ConfigChangeIndex, 1);
  EXPECT_FALSE(cinfo.Pending);
  EXPECT_EQ(info.LogInfo.size(), 0);
}

TEST_F(NodeHostTest, HasNodeInfo)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  EXPECT_FALSE(nh_->HasNodeInfo(1, 1));
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  EXPECT_TRUE(nh_->HasNodeInfo(1, 1));
}

TEST_F(NodeHostTest, ProposalAndRead)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_EQ(code, i + 1);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  dragonboat::Status readStatus = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_EQ(*(uint64_t *)result.Data(), 16);
  EXPECT_TRUE(readStatus.OK());
}

TEST_F(NodeHostTest, OverloadedProposalAndRead)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Byte buf[128];
  for (uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    s = nh_->SyncPropose(cs.get(), buf, 128, timeout, &code);
    EXPECT_EQ(code, i + 1);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  dragonboat::Byte query[128];
  dragonboat::Byte result[128];
  size_t written;
  dragonboat::Status
    readStatus = nh_->SyncRead(1, query, 128, result, 128, &written, timeout);
  EXPECT_TRUE(readStatus.OK());
  EXPECT_EQ(*(uint64_t *)result, 16);
  EXPECT_EQ(written, 8);
}

TEST_F(NodeHostTest, TooSmallTimeoutIsReported)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  int retry = 5;
  bool done = false;
  while (retry > 0) {
    auto timeout = dragonboat::Milliseconds(5);
    std::unique_ptr<dragonboat::Session>
      cs(nh_->SyncGetSession(1, timeout, &s));
    if (s.Code() == dragonboat::Status::ErrInvalidDeadline) {
      retry--;
      continue;
    } else {
      EXPECT_FALSE(s.OK());
      EXPECT_EQ(s.Code(), dragonboat::Status::ErrTimeoutTooSmall);
      done = true;
      break;
    }
  }
  EXPECT_TRUE(done);
}

TEST_F(NodeHostTest, TooBigPayloadIsReported)
{
  auto config = getTestConfig();
  config.MaxInMemLogSize = 1024 * 1024;
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::UpdateResult code;
  int sz = 1024 * 1024 * 2;
  dragonboat::Buffer buf(sz);
  s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
  EXPECT_FALSE(s.OK());
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrPayloadTooBig);
}

TEST_F(NodeHostTest, TooSmallReadBufferIsReported)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(2);
  waitForElectionToComplete();
  dragonboat::Status readStatus = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_FALSE(readStatus.OK());
  EXPECT_EQ(readStatus.Code(), dragonboat::Status::ErrResultBufferTooSmall);
}

TEST_F(NodeHostTest, NodeCanBeAdded)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status
    s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  s = nh_->SyncRequestAddNode(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  for (uint64_t i = 0; i < 4; i++) {
    dragonboat::UpdateResult code;
    auto shortTimeout = dragonboat::Milliseconds(1000);
    s = nh_->SyncPropose(cs.get(), buf, shortTimeout, &code);
    EXPECT_FALSE(s.OK());
    EXPECT_EQ(s.Code(), dragonboat::Status::ErrTimeout);
    cs->ProposalCompleted();
  }
}

TEST_F(NodeHostTest, NodeCanBeAsyncAdded)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status
    s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestAddNode(1, 2, "localhost:9051", timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::RequestResult result = state->Get();
  EXPECT_EQ(result.code, RequestCompleted);
  dragonboat::RequestResult resultAgain = state->Get();
  EXPECT_EQ(resultAgain.code, RequestCompleted);
  for (uint64_t i = 0; i < 4; i++) {
    dragonboat::UpdateResult code;
    auto shortTimeout = dragonboat::Milliseconds(1000);
    s = nh_->SyncPropose(cs.get(), buf, shortTimeout, &code);
    EXPECT_FALSE(s.OK());
    EXPECT_EQ(s.Code(), dragonboat::Status::ErrTimeout);
    cs->ProposalCompleted();
  }
}

TEST_F(NodeHostTest, FailedToLaunchAsyncAddNode)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status
    s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  timeout = dragonboat::Milliseconds(1);
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestAddNode(1, 2, "localhost:9051", timeout, &s));
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrTimeoutTooSmall);
  EXPECT_FALSE(state);
}

TEST_F(NodeHostTest, RemoveData)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status
    s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  s = nh_->SyncRemoveData(1, 1, timeout);
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrClusterNotStopped);
  s = nh_->StopNode(1, 1);
  EXPECT_TRUE(s.OK());
  s = nh_->SyncRemoveData(1, 1, timeout);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, AsyncPropose)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 1; i < 16; i++) {
    TestEvent e;
    s = nh_->Propose(cs.get(), buf, timeout, &e);
    EXPECT_TRUE(s.OK());
    e.Wait();
    dragonboat::RequestResult r = e.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    EXPECT_EQ(r.result, i);
    cs->ProposalCompleted();
  }
}

TEST_F(NodeHostTest, OverloadedAsyncPropose)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Byte buf[128];
  for (uint64_t i = 1; i < 16; i++) {
    TestEvent e;
    s = nh_->Propose(cs.get(), buf, 128, timeout, &e);
    EXPECT_TRUE(s.OK());
    e.Wait();
    dragonboat::RequestResult r = e.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    EXPECT_EQ(r.result, i);
    cs->ProposalCompleted();
  }
}

TEST_F(NodeHostTest, AsyncReadIndex)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 1; i < 16; i++) {
    TestEvent e;
    s = nh_->Propose(cs.get(), buf, timeout, &e);
    EXPECT_TRUE(s.OK());
    e.Wait();
    dragonboat::RequestResult r = e.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    EXPECT_EQ(r.result, i);
    cs->ProposalCompleted();
    TestEvent e2;
    s = nh_->ReadIndex(1, timeout, &e2);
    EXPECT_TRUE(s.OK());
    e2.Wait();
    r = e2.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    dragonboat::Buffer query(128);
    dragonboat::Buffer result(128);
    s = nh_->ReadLocal(1, query, &result);
    EXPECT_TRUE(s.OK());
    uint64_t *count = (uint64_t *) (result.Data());
    EXPECT_EQ(*count, i);
  }
}

TEST_F(NodeHostTest, OverloadedAsyncReadIndex)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 1; i < 16; i++) {
    TestEvent e;
    s = nh_->Propose(cs.get(), buf, timeout, &e);
    EXPECT_TRUE(s.OK());
    e.Wait();
    dragonboat::RequestResult r = e.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    EXPECT_EQ(r.result, i);
    cs->ProposalCompleted();
    TestEvent e2;
    s = nh_->ReadIndex(1, timeout, &e2);
    EXPECT_TRUE(s.OK());
    e2.Wait();
    r = e2.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    dragonboat::Byte query[16];
    dragonboat::Byte result[16];
    size_t written;
    s = nh_->ReadLocal(1, query, 16, result, 16, &written);
    EXPECT_TRUE(s.OK());
    uint64_t *count = (uint64_t *) (result);
    EXPECT_EQ(*count, i);
    EXPECT_EQ(written, 8);
  }
}

TEST_F(NodeHostTest, TooSmallReadBufferForReadIndexIsReported)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::Buffer query1(128);
  dragonboat::Buffer result1(256);
  waitForElectionToComplete();
  dragonboat::Status readStatus = nh_->SyncRead(1, query1, &result1, timeout);
  EXPECT_TRUE(readStatus.OK());
  dragonboat::Buffer query2(128);
  dragonboat::Buffer result2(2);
  readStatus = nh_->ReadLocal(1, query2, &result2);
  EXPECT_FALSE(readStatus.OK());
  EXPECT_EQ(readStatus.Code(), dragonboat::Status::ErrResultBufferTooSmall);
}

TEST_F(NodeHostTest, StaleRead)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::Buffer buf(128);
  for(uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_EQ(code, i + 1);
    EXPECT_TRUE(s.OK());
  }
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  dragonboat::Status staleReadStatus = nh_->StaleRead(1, query, &result);
  EXPECT_TRUE(staleReadStatus.OK());
  EXPECT_EQ(*(uint64_t*)result.Data(), 16);
}

TEST_F(NodeHostTest, OverloadedStaleRead)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::Buffer buf(128);
  for(uint64_t i = 0; i < 16; i++) {
    dragonboat::UpdateResult code;
    s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_EQ(code, i + 1);
    EXPECT_TRUE(s.OK());
  }
  dragonboat::Byte query[128];
  dragonboat::Byte result[128];
  size_t written;
  dragonboat::Status
    staleReadStatus = nh_->StaleRead(1, query, 128, result, 128, &written);
  EXPECT_TRUE(staleReadStatus.OK());
  EXPECT_EQ(written, 8);
  EXPECT_EQ(*(uint64_t*)result, 16);
}

TEST_F(NodeHostTest, AsyncSessionProposal)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session>
    cs(dragonboat::Session::GetNewSession(1));
  cs->PrepareForRegistration();
  TestEvent e;
  s = nh_->ProposeSession(cs.get(), timeout, &e);
  EXPECT_TRUE(s.OK());
  e.Wait();
  dragonboat::RequestResult r = e.Get();
  EXPECT_EQ(r.code, RequestCompleted);
  cs->PrepareForProposal();
  dragonboat::Buffer buf(128);
  for (uint64_t i = 1; i < 16; i++) {
    TestEvent e2;
    s = nh_->Propose(cs.get(), buf, timeout, &e2);
    EXPECT_TRUE(s.OK());
    e2.Wait();
    r = e2.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    EXPECT_EQ(r.result, i);
    cs->ProposalCompleted();
    TestEvent e3;
    s = nh_->ReadIndex(1, timeout, &e3);
    EXPECT_TRUE(s.OK());
    e3.Wait();
    r = e3.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    dragonboat::Buffer query(128);
    dragonboat::Buffer result(128);
    s = nh_->ReadLocal(1, query, &result);
    EXPECT_TRUE(s.OK());
    uint64_t *count = (uint64_t *) (result.Data());
    EXPECT_EQ(*count, i);
  }
  cs->PrepareForUnregistration();
  TestEvent e1;
  s = nh_->ProposeSession(cs.get(), timeout, &e1);
  EXPECT_TRUE(s.OK());
  e1.Wait();
  r = e1.Get();
  EXPECT_EQ(r.code, RequestCompleted);
}

TEST_F(NodeHostTest, NoOPSession)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::Buffer buf(128);
  for (uint64_t i = 1; i < 16; i++) {
    TestEvent e2;
    s = nh_->Propose(cs.get(), buf, timeout, &e2);
    EXPECT_TRUE(s.OK());
    e2.Wait();
    dragonboat::RequestResult r = e2.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    EXPECT_EQ(r.result, i);
    TestEvent e3;
    s = nh_->ReadIndex(1, timeout, &e3);
    EXPECT_TRUE(s.OK());
    e3.Wait();
    r = e3.Get();
    EXPECT_EQ(r.code, RequestCompleted);
    dragonboat::Buffer query(128);
    dragonboat::Buffer result(128);
    s = nh_->ReadLocal(1, query, &result);
    EXPECT_TRUE(s.OK());
    uint64_t *count = (uint64_t *) (result.Data());
    EXPECT_EQ(*count, i);
  }
}

TEST_F(NodeHostTest, RequestSnapshot)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::SnapshotOption option;
  dragonboat::SnapshotResultIndex result;
  option.Exported = false;
  dragonboat::Status
    status = nh_->SyncRequestSnapshot(1, option, timeout, &result);
  EXPECT_TRUE(status.OK());
  EXPECT_GE(result, 1);
  EXPECT_TRUE(snapshotExist(NodeHostTestDir));
}

TEST_F(NodeHostTest, AsyncRequestSnapshot)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::SnapshotOption option;
  dragonboat::Status status;
  option.Exported = false;
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestSnapshot(1, option, timeout, &status));
  EXPECT_TRUE(status.OK());
  dragonboat::RequestResult result = state->Get();
  EXPECT_EQ(result.code, RequestCompleted);
  EXPECT_GE(result.result, 1);
  EXPECT_TRUE(snapshotExist(NodeHostTestDir));
}

TEST_F(NodeHostTest, ExportSnapshot)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::SnapshotOption option;
  dragonboat::SnapshotResultIndex result;
  option.Exported = true;
  option.ExportedPath = ExportedSnapshotDir;
  dragonboat::Status
    status = nh_->SyncRequestSnapshot(1, option, timeout, &result);
  EXPECT_TRUE(status.OK());
  EXPECT_GE(result, 1);
  EXPECT_TRUE(snapshotExist(ExportedSnapshotDir));
}

TEST_F(NodeHostTest, AsyncExportSnapshot)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  auto timeout = dragonboat::Milliseconds(5000);
  dragonboat::SnapshotOption option;
  dragonboat::Status status;
  option.Exported = true;
  option.ExportedPath = ExportedSnapshotDir;
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestSnapshot(1, option, timeout, &status));
  EXPECT_TRUE(status.OK());
  dragonboat::RequestResult result = state->Get();
  EXPECT_EQ(result.code, RequestCompleted);
  EXPECT_GE(result.result, 1);
  EXPECT_TRUE(snapshotExist(ExportedSnapshotDir));
}

TEST_F(NodeHostTest, FailedToLaunchAsyncSnapshot)
{
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  auto config = getTestConfig();
  dragonboat::Status s = nh_->StartCluster(p, false,
    CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  auto timeout = dragonboat::Milliseconds(1);
  dragonboat::SnapshotOption option;
  dragonboat::Status status;
  option.Exported = false;
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestSnapshot(1, option, timeout, &status));
  EXPECT_EQ(status.Code(), dragonboat::Status::ErrTimeoutTooSmall);
  EXPECT_FALSE(state);
}

TEST_F(NodeHostTest, RegularSMSnapshotCanBeCapturedAndRestored)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 64; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  nh_->Stop();
  auto nhConfig = getTestNodeHostConfig();
  nh_.reset(new dragonboat::NodeHost(nhConfig));
  s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(result.Len(), 8);
  uint64_t *count = (uint64_t *) (result.Data());
  // applied index is 66, one is the empty entry proposed after the leader is
  // elected, one is the membership change entry. both of these two are not
  // visible to the StateMachine, so the returned count is 64
  EXPECT_EQ(*count, 64);
}

TEST_F(NodeHostTest, RegularSMSnapshotUsingSnappyCanBeCapturedAndRestored)
{
  auto config = getTestConfig();
  config.SnapshotCompressionType = SNAPPY;
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 64; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  nh_->Stop();
  auto nhConfig = getTestNodeHostConfig();
  nh_.reset(new dragonboat::NodeHost(nhConfig));
  s = nh_->StartCluster(p, false, CreateRegularStateMachine, config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(result.Len(), 8);
  uint64_t *count = (uint64_t *) (result.Data());
  // applied index is 66, one is the empty entry proposed after the leader is
  // elected, one is the membership change entry. both of these two are not
  // visible to the StateMachine, so the returned count is 64
  EXPECT_EQ(*count, 64);
}

TEST_F(NodeHostTest, ConcurrentSMSnapshotCanBeCapturedAndRestored)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateConcurrentStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->SyncGetSession(1, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 64; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
    cs->ProposalCompleted();
  }
  nh_->Stop();
  auto nhConfig = getTestNodeHostConfig();
  nh_.reset(new dragonboat::NodeHost(nhConfig));
  s = nh_->StartCluster(p, false, CreateConcurrentStateMachine, config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(result.Len(), 8);
  uint64_t *count = (uint64_t *) (result.Data());
  // applied index is 66, one is the empty entry proposed after the leader is
  // elected, one is the membership change entry. both of these two are not
  // visible to the StateMachine, so the returned count is 64
  EXPECT_EQ(*count, 64);
}

TEST_F(NodeHostTest, OnDiskSMSnapshotCanBeCapturedAndRestored)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  uint64_t lastRaftIndex = 0;
  dragonboat::Status s = nh_->StartCluster(
    p, false,
    [&lastRaftIndex](uint64_t clusterID, uint64_t nodeID) {
      return new TestOnDiskStateMachine(clusterID, nodeID, &lastRaftIndex);
    },
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(5000);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 64; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
  }
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(result.Len(), 8);
  // initial applied index is 64,
  // plus one empty entry proposed after the leader is elected and one
  // membership change entry. both of these two are not
  // visible to the StateMachine, so the returned count is 64, and the index
  // of the most recent Raft log is 66
  EXPECT_EQ(*(uint64_t *)result.Data(), 64);
  EXPECT_EQ(lastRaftIndex, 66);
  nh_->Stop();
  auto nhConfig = getTestNodeHostConfig();
  nh_.reset(new dragonboat::NodeHost(nhConfig));
  // the on-disk state machine must persist the most recent index of the Raft
  // log which is 66 in this test, thus a new sm should start with index = 66
  s = nh_->StartCluster(p, false,
    [](uint64_t clusterID, uint64_t nodeID){
      return new TestOnDiskStateMachine(clusterID, nodeID, 66);
    }, config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(result.Len(), 8);
  uint64_t *count = (uint64_t *) (result.Data());
  // on-disk state machine only saves a dummy snapshot because it is supposed to
  // have all data persisted
  EXPECT_EQ(*count, 0);
}

TEST_F(NodeHostTest, OnDiskSMSnapshotCanBeStreamed)
{
  auto config = getTestConfig();
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  uint64_t lastRaftIndex1 = 0;
  dragonboat::Status s = nh_->StartCluster(
    p, false,
    [&lastRaftIndex1](uint64_t clusterID, uint64_t nodeID) {
      return new TestOnDiskStateMachine(clusterID, nodeID, &lastRaftIndex1);
    },
    config);
  EXPECT_TRUE(s.OK());
  auto timeout = dragonboat::Milliseconds(500);
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  EXPECT_TRUE(s.OK());
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 64; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
  }
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(result.Len(), 8);
  // initial applied index is 64,
  // plus one empty entry proposed after the leader is elected and one
  // membership change entry. both of these two are not
  // visible to the StateMachine, so the returned count is 64, and the index
  // of the most recent Raft log is 66
  EXPECT_EQ(*(uint64_t *)result.Data(), 64);
  EXPECT_EQ(lastRaftIndex1, 66);
  // add a new node prepared for streamSnapshot
  std::cout << "going to add node" << std::endl;
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestAddNode(1, 2, "localhost:9051", timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::RequestResult reqresult = state->Get();
  EXPECT_EQ(reqresult.code, RequestCompleted);

  dragonboat::Peers p2;
  uint64_t lastRaftIndex2 = 0;
  auto config2 = getTestConfig();
  config2.NodeId = 2;
  std::cout << "going to start on-disk state machine"
    << ", snapshot should be streamed" << std::endl;
  s = nh2_->StartCluster(
    p2, true,
    [&lastRaftIndex2](uint64_t clusterID, uint64_t nodeID) {
      return new TestOnDiskStateMachine(clusterID, nodeID, &lastRaftIndex2);
    },
    config2);
  EXPECT_TRUE(s.OK());
  for (uint64_t i = 0; i < 10; i++) {
    s = nh2_->SyncRead(1, query, &result, timeout);
    std::cout << "try read iteration " << i << std::endl;
    if (s.OK()) {
      break;
    }
  }
  EXPECT_TRUE(s.OK());
  EXPECT_EQ(*(uint64_t*)result.Data(), 64);
  EXPECT_TRUE(snapshotExist(NodeHostTestDir));
  EXPECT_TRUE(snapshotExist(NodeHostTestDir2));
  for (uint64_t i = 0; i < 1; i++) {
    dragonboat::UpdateResult code;
    dragonboat::Status s = nh2_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
  }
  // Snapshot index is 67, 66 + 1 membership change entry which is not visible
  // to the StateMachine. After the nh2_->SyncPropose(), the index of the most
  // recent Raft log is 68.
  EXPECT_EQ(lastRaftIndex1, 68);
  EXPECT_EQ(lastRaftIndex2, 68);
}

TEST_F(NodeHostTest, ObserverCanBeAdded)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::UpdateResult code;
  dragonboat::Buffer buf(128);
  s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
  EXPECT_EQ(code, uint64_t(1));
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, ObserverCanBeAsyncAdded)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestAddObserver(1, 2, "localhost:9051", timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::RequestResult result = state->Get();
  EXPECT_EQ(result.code, RequestCompleted);
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::UpdateResult code;
  dragonboat::Buffer buf(128);
  s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
  EXPECT_EQ(code, uint64_t(1));
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, FailedToLaunchAsyncAddObserver)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(1);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestAddObserver(1, 2, "localhost:9051", timeout, &s));
  EXPECT_EQ(s.Code(), dragonboat::Status::ErrTimeoutTooSmall);
  EXPECT_FALSE(state);
}

TEST_F(NodeHostTest, ObserverCanBeRemoved)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  s = nh_->SyncRequestDeleteNode(1, 2, timeout);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, ObserverCanBeAsyncRemoved)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  std::unique_ptr<dragonboat::RequestState>
    state(nh_->RequestDeleteNode(1, 2, timeout, &s));
  EXPECT_TRUE(s.OK());
  dragonboat::RequestResult result = state->Get();
  EXPECT_EQ(result.code, RequestCompleted);
}

TEST_F(NodeHostTest, ObserverCanBePromoted)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::UpdateResult code;
  dragonboat::Buffer buf(128);
  s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
  EXPECT_EQ(code, uint64_t(1));
  EXPECT_TRUE(s.OK());
  std::cout << "going to add node" << std::endl;
  s = nh_->SyncRequestAddNode(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  s = nh_->SyncPropose(cs.get(), buf, timeout, &code);
  EXPECT_FALSE(s.OK());
}

TEST_F(NodeHostTest, ObserverCanSyncPropose)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  dragonboat::Peers p2;
  auto config2 = getTestConfig();
  config2.NodeId = 2;
  config2.IsObserver = true;
  s = nh2_->StartCluster(p2, true, CreateRegularStateMachine, config2);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete(true);
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::UpdateResult code;
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 5; i++) {
    s = nh2_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
  }
}

TEST_F(NodeHostTest, ObserverCanReadIndex)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  dragonboat::Peers p2;
  auto config2 = getTestConfig();
  config2.NodeId = 2;
  config2.IsObserver = true;
  s = nh2_->StartCluster(p2, true, CreateRegularStateMachine, config2);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete(true);
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::UpdateResult code;
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 5; i++) {
    std::cout << "make proposal iteration " << i << std::endl;
    s = nh2_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
  }
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh2_->SyncRead(1, query, &result, timeout);
  EXPECT_TRUE(s.OK());
}

TEST_F(NodeHostTest, ObserverCanStaleRead)
{
  auto config = getTestConfig();
  auto timeout = dragonboat::Milliseconds(2000);
  dragonboat::Peers p;
  p.AddMember("localhost:9050", 1);
  dragonboat::Status s = nh_->StartCluster(
    p, false, CreateRegularStateMachine,
    config);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete();
  s = nh_->SyncRequestAddObserver(1, 2, "localhost:9051", timeout);
  EXPECT_TRUE(s.OK());
  dragonboat::Peers p2;
  auto config2 = getTestConfig();
  config2.NodeId = 2;
  config2.IsObserver = true;
  s = nh2_->StartCluster(p2, true, CreateRegularStateMachine, config2);
  EXPECT_TRUE(s.OK());
  waitForElectionToComplete(true);
  std::unique_ptr<dragonboat::Session> cs(nh_->GetNoOPSession(1));
  dragonboat::UpdateResult code;
  dragonboat::Buffer buf(128);
  for (uint64_t i = 0; i < 5; i++) {
    std::cout << "make proposal iteration " << i << std::endl;
    s = nh2_->SyncPropose(cs.get(), buf, timeout, &code);
    EXPECT_TRUE(s.OK());
  }
  dragonboat::Buffer query(128);
  dragonboat::Buffer result(128);
  s = nh2_->StaleRead(1, query, &result);
  EXPECT_TRUE(s.OK());
}
