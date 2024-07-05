#pragma once
#include <memory>

#include "loggers/loggers_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace noisepage {

class TerrierTest : public ::testing::Test {
public:
    TerrierTest() {
        LoggersUtil::Initialize();
    }

    ~TerrierTest() override {
        LoggersUtil::ShutDown();
    }
};

} // namespace noisepage
