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

struct MsMacroQuery {
    uint32_t id;
    std::string text;
    std::vector<uint32_t> dim_ids;
    std::vector<float> weights;
    std::vector<uint32_t> neighbors;
    std::vector<float> distances;
};


template<typename F>
class RowParseHandler : public BaseReaderHandler<UTF8<>, RowParseHandler<F>> 
{
public:
    RowParseHandler(F func, size_t limit_rows) : inDimIds(false), inWeights(false), callback(func), limit_rows(limit_rows), row_count(0) {}

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


template<typename F>
class QueryParseHandler : public BaseReaderHandler<UTF8<>, QueryParseHandler<F>> 
{
public:
    QueryParseHandler(F func, size_t limit_rows) : inDimIds(false), inWeights(false), inNeighbors(false), inDistances(false), callback(func), limit_rows(limit_rows), row_count(0) {}

    bool StartObject() {
        std::cout<<"StartObject row_count "<< row_count <<" limit rows "<< limit_rows <<std::endl;

        if (limit_rows > 0 && row_count >= limit_rows) {
            return false;
        }
        std::cout<<"StartObject OK"<<std::endl;
        currentQuery = MsMacroQuery();
        return true;
    }

    bool EndObject(SizeType) {
        callback(currentQuery);
        row_count++;
        return true;
    }

    bool Key(const char* str, SizeType length, bool copy) {
        currentKey = std::string(str, length);
        return true;
    }

    bool Uint(unsigned u) {
        if (currentKey == "id") {
            currentQuery.id = u;
        } else if (inDimIds) {
            currentQuery.dim_ids.push_back(u);
        } else if (inNeighbors) {
            currentQuery.neighbors.push_back(u);
        }
        return true;
    }

    bool Int(int i) {
        if (inDimIds) {
            currentQuery.dim_ids.push_back(static_cast<uint32_t>(i));
        } else if (inNeighbors) {
            currentQuery.neighbors.push_back(static_cast<uint32_t>(i));
        }
        return true;
    }

    bool String(const char* str, SizeType length, bool copy) {
        if (currentKey == "text") {
            currentQuery.text = std::string(str, length);
        }
        return true;
    }

    bool StartArray() {
        if (currentKey == "dim_ids") {
            inDimIds = true;
        } else if (currentKey == "weights") {
            inWeights = true;
        } else if (currentKey == "neighbors") {
            inNeighbors = true;
        } else if (currentKey == "distances") {
            inDistances = true;
        }
        return true;
    }

    bool EndArray(SizeType) {
        inDimIds = false;
        inWeights = false;
        inNeighbors = false;
        inDistances = false;
        return true;
    }

    bool Double(double d) {
        if (inWeights) {
            currentQuery.weights.push_back(d);
        } else if (inDistances) {
            currentQuery.distances.push_back(d);
        }
        return true;
    }

private:
    MsMacroQuery currentQuery;
    std::string currentKey;
    bool inDimIds;
    bool inWeights;
    bool inNeighbors;
    bool inDistances;
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


    template<typename F>
    void iterateTrainRows(F f, size_t limit_rows = -1) {
        FILE* fp = fopen(this->train_file.c_str(), "rb"); // "rb" 表示以二进制方式读取
        if (!fp) {
            std::cerr << "Unable to open file " << this->train_file << std::endl;
        } else {
            std::cout<<"open `"<< this->query_file <<"` success"<<std::endl;
        }
        
        char readBuffer[65536];
        rapidjson::FileReadStream is(fp, readBuffer, sizeof(readBuffer));

        Reader reader;
        RowParseHandler<F> handler(f, limit_rows);

        reader.Parse(is, handler);
        fclose(fp); // 关闭文件
    }

    template<typename F>
    void iterateQueryRows(F f, size_t limit_rows = -1) {
        FILE* fp = fopen(this->query_file.c_str(), "rb"); // "rb" 表示以二进制方式读取
        if (!fp) {
            std::cerr << "Unable to open file " << this->query_file << std::endl;
        } else {
            std::cout<<"open `"<< this->query_file <<"` success"<<std::endl;
        }
        
        char readBuffer[65536];
        rapidjson::FileReadStream is(fp, readBuffer, sizeof(readBuffer));

        Reader reader;
        QueryParseHandler<F> handler(f, limit_rows);

        reader.Parse(is, handler);
        fclose(fp); // 关闭文件
    }

private:
    MsMacroLoader(){};

    std::string query_file;
    std::string train_file;
};
