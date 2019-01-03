// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

#include <memory>
#include "dragonboat/dragonboat.h"
#include "gtest/gtest.h"

TEST(DragonboatTest, StatusCanBeSet)
{
  dragonboat::Status s1(dragonboat::Status::StatusOK);
  EXPECT_TRUE(s1.OK());
  EXPECT_EQ(s1.Code(), dragonboat::Status::StatusOK);
  dragonboat::Status s2(dragonboat::Status::ErrTimeout);
  EXPECT_FALSE(s2.OK());
  EXPECT_EQ(s2.Code(), dragonboat::Status::ErrTimeout);
  dragonboat::Status s3(dragonboat::Status::ErrClusterNotFound);
  EXPECT_FALSE(s3.OK());
  EXPECT_EQ(s3.Code(), dragonboat::Status::ErrClusterNotFound);
}

TEST(DragonboatTest, PeersCanBeSet)
{
  dragonboat::Peers p1;
  EXPECT_EQ(0, p1.Len());
  p1.AddMember("localhost:9010", 1);
  p1.AddMember("localhost:9011", 2);
  p1.AddMember("localhost:9011", 3);
  EXPECT_EQ(2, p1.Len());
  auto r = p1.GetMembership();
  EXPECT_EQ(r.size(), 2);
  EXPECT_EQ(r["localhost:9010"], 1);
  EXPECT_EQ(r["localhost:9011"], 3);
}

TEST(DragonboatTest, BufferCanBeSet)
{
  dragonboat::Buffer buf(128);
  EXPECT_EQ(buf.Len(), 128);
  EXPECT_EQ(buf.Capacity(), 128);
  buf.SetLen(32);
  EXPECT_EQ(buf.Len(), 32);
  EXPECT_EQ(buf.Capacity(), 128);
  buf.SetLen(256);
  EXPECT_EQ(buf.Len(), 32);
  EXPECT_EQ(buf.Capacity(), 128);
}

TEST(DragonboatTest, BufferContentCanBeSet)
{
  std::unique_ptr<dragonboat::Byte[]> b(new dragonboat::Byte[128]);
  for(int i = 0; i < 128; i++)
  {
    b[i] = 0xFE;
  }
  dragonboat::Buffer buf(b.get(), 128);
  EXPECT_EQ(buf.Len(), 128);
  EXPECT_EQ(buf.Capacity(), 128);
  for(int i = 0; i < 128; i++)
  {
    EXPECT_EQ(buf.Data()[i], 0xFE);
  }
}

TEST(DragonboatTest, LoggerCanBeSet)
{
  namespace db = dragonboat;
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Multiraft, db::Logger::LOG_LEVEL_INFO));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Raft, db::Logger::LOG_LEVEL_INFO));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::LogDB, db::Logger::LOG_LEVEL_INFO));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::RSM, db::Logger::LOG_LEVEL_INFO));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Transport, db::Logger::LOG_LEVEL_INFO));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Transport, db::Logger::LOG_LEVEL_ERROR));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Transport, db::Logger::LOG_LEVEL_WARNING));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Transport, db::Logger::LOG_LEVEL_INFO));
  EXPECT_TRUE(db::Logger::SetLogLevel(db::Logger::Transport, db::Logger::LOG_LEVEL_DEBUG));
}
