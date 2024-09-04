// #include <MsMacroLoader.h>

// void from_json(const json &j, MsMacorRow &row)
// {
//     j.at("row_id").get_to(row.row_id);
//     j.at("text").get_to(row.text);
//     j.at("dim_ids").get_to(row.dim_ids);
//     j.at("weights").get_to(row.weights);
// }


// std::vector<uint64_t> generate_rowid_range(std::size_t step, std::size_t lrange, std::size_t rrange) {
//     std::vector<uint64_t> array;
//     std::size_t size = (rrange - lrange) / step + 1; 
//     array.reserve(size);

//     for (uint64_t i = lrange; i <= rrange; i += step) {
//         array.push_back(i);
//     }

//     return array;
// }

// size_t index_data_from_json_file(const std::string &raw_docs_file_path, const std::string &index_files_directory)
// {
//     // recreate directory
//     if (fs::exists(index_files_directory)) {
//         fs::remove_all(index_files_directory);
//     }
//     if (!fs::create_directories(index_files_directory)) {
//         std::cerr << "Failed to create directory: " << index_files_directory << std::endl;
//     }
//     // load rows
//     std::vector<MsMacroRow> rows = MsMacroLoader::getInstance().loadRows(raw_docs_file_path);
//     // ffi_create_index(index_files_directory, {"text"});

//     // index all docs
//     size_t row_id = 0;
//     for (const auto &row : rows)
//     {
//         // ffi_index_multi_column_docs(index_files_directory, row_id,  {"text"}, {doc.body.c_str()});
//         row_id += 1;
//     }
//     // ffi_index_writer_commit(index_files_directory);
//     // ffi_free_index_writer(index_files_directory);
//     return row_id;
// }

// Dataset loader for wiki dataset
// MsMacroLoader& MsMacroLoader::getInstance() {
//     static MsMacroLoader instance;
//     return instance;
// }

// void MsMacroLoader::init(const std::string& train_file, const std::string& query_file)
// {
//     this->query_file = query_file;
//     this->train_file = train_file;   
// }

// std::vector<std::string> MsMacroLoader::loadQueryTerms(const std::string& file_path) {
//     // thread-safe
//     std::lock_guard<std::mutex> lock(query_terms_mutex);
//     // different file path, reload
//     if (file_path!=query_terms_file_path){
//         std::ifstream file(file_path);
//         json j;
//         file >> j;
//         query_terms = j["terms"];
//         file.close();
//         query_terms_file_path = file_path;
//     }
//     return query_terms;
// }

// std::vector<std::string> MsMacroLoader::loadQueryTerms(){
//     return loadQueryTerms(query_terms_file_path);
// }


// void MsMacroLoader::setIndexDirectory(const std::string& index_directory) {
//         this->index_directory = index_directory;
//     }
// std::string MsMacroLoader::getIndexDirectory(){
//     return this->index_directory;
// }

// void MsMacroLoader::setDatasetFilePath(const std::string& dataset_file_path){
//     this->dataset_file_path = dataset_file_path;
// }

// std::string MsMacroLoader::getDatasetFilePath(){
//     return this->dataset_file_path;
// }

// std::vector<uint64_t> MsMacroLoader::getRowIdRanges(size_t index_granularity){
//     return generate_rowid_range(index_granularity, 0, wiki_total_docs);
// }

// std::vector<size_t> MsMacroLoader::generateRandomArray(int length, int min, int max) {
//     if (min > max) std::swap(min, max);
//     std::mt19937 rng(static_cast<unsigned int>(time(nullptr)));
//     std::uniform_int_distribution<int> dist(min, max);
//     std::vector<size_t> randomArray;
//     randomArray.reserve(length);

//     for (int i = 0; i < length; ++i) {
//         randomArray.push_back(dist(rng));
//     }

//     return randomArray;
// }
