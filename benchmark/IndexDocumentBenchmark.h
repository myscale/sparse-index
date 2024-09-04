#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <fstream>
#include <nlohmann/json.hpp>
#include <sparse_index.h>
#include <random>
#include <sstream>
#include <iomanip>
#include <unistd.h>
#include <boost/program_options.hpp>
#include <benchmark/benchmark.h>
#include <MsMacroLoader.h>
#include <filesystem>

using json = nlohmann::json;

using namespace std;
namespace fs = std::filesystem;


class IndexOneColumn : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        std::cout<<"hi"<<std::endl;
    }

    void TearDown(const ::benchmark::State& state) override {

    }

    void IndexOneColumn1MDoc(benchmark::State& state) {

    }  
};

