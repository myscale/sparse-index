#include <BM25SearchBenchmark.h>
#include <IndexDocumentBenchmark.h>
#include <benchmark/benchmark.h>
#include <sparse_index.h>
using namespace std;
using namespace SPARSE;
namespace  bpo = boost::program_options;

template<typename T>
void printArray(const std::vector<T>& arr) {
    for (const auto& element : arr) {
        std::cout << element << " ";
    }
    std::cout << std::endl;
}

// Run the benchmark
int main(int argc, char** argv) {
    string index_path;
    string train_file;
    string query_file;
    size_t train_rows_limit;
    bool skip_build_index;

    bpo::options_description desc("Benchmark Options");
    desc.add_options()
    ("index-path,ip", bpo::value<std::string>(&index_path)->default_value("/tmp/sparse_index/benchmark/index_path"), "tantivy index files directory")
    ("query-file,qf", bpo::value<std::string>(&query_file)->default_value("ms-macro-sparse-test.json"), "query json file path")
    ("train-file,tf", bpo::value<std::string>(&train_file)->default_value("ms-macro-sparse-train.json"), "train json file path")
    ("train-rows-limit,trl", bpo::value<size_t>(&train_rows_limit)->default_value(-1), "train rows limit")
    ("skip-build-index,sbi", bpo::value<bool>(&skip_build_index)->default_value(false), "if need skip build index")
    ("help", "this is help message");


   try {
        bpo::variables_map vm;
        bpo::store(bpo::parse_command_line(argc, argv, desc), vm);
        bpo::notify(vm);
        if(vm.count("help")) {
            return 0;
        }


        char arg0_default[] = "benchmark";
        char* args_default = arg0_default;
        if (!argv) {
        argc = 1;
        argv = &args_default;
        }
        std::cout<<"hello"<<std::endl;

        MsMacroLoader& loader = MsMacroLoader::getInstance();
        loader.init(train_file, query_file);

        // Prepare for benchmark.
        if (!skip_build_index){
            std::cout << "Create index...." << endl;
            ffi_create_index_with_parameter(index_path, "{}");
            std::cout << "Build index..." << endl;

            loader.iterateTrainRows([&](const MsMacroRow& row) {
                std::cout << row.row_id << std::endl;
                rust::Vec<TupleElement> sparse_vector;
                for (size_t i = 0; i < row.dim_ids.size(); i++) {
                    sparse_vector.emplace_back(
                        TupleElement{
                            row.dim_ids[i],
                            row.weights[i],
                            0,
                            0,
                            0
                        }
                    );
                }
                ffi_insert_sparse_vector(index_path, row.row_id, sparse_vector);
            }, train_rows_limit);
            std::cout << "Commit index..." << endl;
            ffi_commit_index(index_path);
        }

        std::cout << "Load index..." << endl;
        ffi_load_index(index_path);

        std::cout << "Search from index..." << endl;
        loader.iterateQueryRows([&](const MsMacroQuery& row) {
            std::cout << row.id << std::endl;
            rust::Vec<TupleElement> sparse_vector;
            for (size_t i = 0; i < row.dim_ids.size(); i++) {
                sparse_vector.emplace_back(
                    TupleElement{
                        row.dim_ids[i],
                        row.weights[i],
                        0,
                        0,
                        0
                    }
                );
            }
            rust::Vec<uint8_t> filter;
            std::cout<<"Hello begin" <<std::endl;
            const auto& result = ffi_sparse_search(
                index_path, 
                sparse_vector,
                filter,
                5
            );
            std::cout<<"Hello" <<std::endl;
            std::cout<<"res is ok? " << !result.error.is_error <<std::endl;
            std::cout<<"res[0].row_id: " << result.result[0].row_id <<std::endl;
        }, 100);

        std::this_thread::sleep_for(std::chrono::seconds(2));

        // MsMacroLoader::getInstance().loadQueryTerms(query_term_path);
        // MsMacroLoader::getInstance().setIndexDirectory(index_path);
        // MsMacroLoader::getInstance().setDatasetFilePath(docs_path);
        // tantivy_search_log4rs_initialize("./log", "info", true, false, false);

        // std::cout<<"try iter train rows"<<std::endl;


        // Run all benchmark
        // int benchmark_argc = 2;
        // char* benchmark_program = argv[0];
        // char benchmark_tabular_arg[] = "--benchmark_counters_tabular=true";
        // char* benchmark_argv[] = { benchmark_program, benchmark_tabular_arg };
        // ::benchmark::Initialize(&benchmark_argc, benchmark_argv);
        // if (::benchmark::ReportUnrecognizedArguments(benchmark_argc, benchmark_argv)) return 1;
        // ::benchmark::RunSpecifiedBenchmarks();
        // ::benchmark::Shutdown();
        return 0;
    } catch (const bpo::error &e) {
        return 1;
    }
}