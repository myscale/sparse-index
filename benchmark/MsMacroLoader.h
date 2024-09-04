#pragma once

// #include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <mutex>
#include <string>
#include <sparse_index.h>
#include <random>
#include <ctime>
#include <rapidjson/reader.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <cstdio>

namespace fs = std::filesystem;
using namespace rapidjson;

struct MsMacroRow {
    uint32_t row_id;
    std::string text;
    std::vector<uint32_t> dim_ids;
    std::vector<float> weights;
};


template<typename F>
class RowParseHandler : public BaseReaderHandler<UTF8<>, RowParseHandler<F>> 
{
public:
    RowParseHandler(F func, size_t limit_rows) : inDimIds(false), inWeights(false), callback(func), limit_rows(limit_rows) {}

    bool StartObject() {
        if (limit_rows > 0 && row_count >= limit_rows) {
            return false;
        }
        currentRow = MsMacroRow();
        return true;
    }

    bool EndObject(SizeType) {
        callback(currentRow);
        row_count++;
        return true;
    }

    bool Key(const char* str, SizeType length, bool copy) {
        currentKey = std::string(str, length);
        return true;
    }

    bool Uint(unsigned u) {
        if (currentKey == "row_id") {
            currentRow.row_id = u;
        } else if (inDimIds) {
            currentRow.dim_ids.push_back(u);
        }
        return true;
    }

    bool Int(int i) {
        if (inDimIds) {
            currentRow.dim_ids.push_back(static_cast<uint32_t>(i));
        }
        return true;
    }

    bool String(const char* str, SizeType length, bool copy) {
        if (currentKey == "text") {
            currentRow.text = std::string(str, length);
        }
        return true;
    }

    bool StartArray() {
        if (currentKey == "dim_ids") {
            inDimIds = true;
        } else if (currentKey == "weights") {
            inWeights = true;
        }
        return true;
    }

    bool EndArray(SizeType) {
        inDimIds = false;
        inWeights = false;
        return true;
    }

    bool Double(double d) {
        if (inWeights) {
            currentRow.weights.push_back(d);
        }
        return true;
    }

private:
    MsMacroRow currentRow;
    std::string currentKey;
    bool inDimIds;
    bool inWeights;
    F callback;
    size_t limit_rows;
    size_t row_count;
};


class MsMacroLoader {
public:
    static MsMacroLoader& getInstance(){
        static MsMacroLoader instance;
        return instance;
    };

    MsMacroLoader(const MsMacroLoader&) = delete;
    void operator=(const MsMacroLoader&) = delete;

    void init(const std::string& train_file, const std::string& query_file)
    {
        this->query_file = query_file;
        this->train_file = train_file;   
    };

    // template<typename F>
    // void iterateTrainRows(F f) {
    //     iterateRows(this->train_file, f);
    // }

    // template<typename F>
    // void iterateQueryRows(F f) {
    //     iterateRows(this->query_file, f);
    // }

        template<typename F>
    void iterateTrainRows(F f, size_t limit_rows = -1) {
        iterateRows(this->train_file, f, limit_rows);
    }

    template<typename F>
    void iterateQueryRows(F f, size_t limit_rows = -1) {
        iterateRows(this->query_file, f, limit_rows);
    }

private:
    MsMacroLoader(){};
    
    template<typename F>
    void iterateRows(const std::string& file_path, F f, size_t limit_rows=-1)
    {
        std::cout<<"begin open file"<<std::endl;
        FILE* fp = fopen(file_path.c_str(), "rb"); // "rb" 表示以二进制方式读取
        if (!fp) {
            std::cerr << "Unable to open file " << file_path << std::endl;
        }
        std::cout<<"open file success"<<std::endl;
        
        char readBuffer[65536];
        rapidjson::FileReadStream is(fp, readBuffer, sizeof(readBuffer));

        Reader reader;
        RowParseHandler<F> handler(f, limit_rows);

        reader.Parse(is, handler);
        fclose(fp); // 关闭文件
    }

    std::string query_file;
    std::string train_file;

    std::vector<MsMacroRow> rows;
};
