#include "mixgraph.h"
#include "workload_factory.h"
#include "two_term_exp_generator.h"
#include "pareto_generator.h"
#include "random_byte_generator.h"
#include "uniform_generator.h"

static std::string build_key(uint64_t key_num, size_t key_size) {
    std::string key_suf = std::to_string(key_num);
    std::string key_pre(key_size > key_suf.size() + 4 ? key_size - key_suf.size() - 4 : 0, '0');
    return "user" + key_pre + key_suf;
}

static std::vector<ycsbc::DB::Field> build_values(uint64_t value_size) {
    ycsbc::DB::Field field;
    field.name = "VF";
    size_t nfill = value_size > 8 + 2 ? value_size - 8 - 2 : 0;
    field.value.resize(nfill);
    ycsbc::RandomByteGenerator byte_gen;
    std::generate_n(field.value.begin(), nfill, [&byte_gen]() { return byte_gen.Next(); });
    return {field};
}

void ycsbc::MixGraph::Init(const utils::Properties &p) {
    double read_proportion = std::stod(p.GetProperty("readproportion", "1"));
    double insert_proportion = std::stod(p.GetProperty("insertproportion", "0"));
    double scan_proportion = std::stod(p.GetProperty("scanproportion", "0"));
    if (read_proportion > 0)
        op_gen.AddValue(READ, read_proportion);
    if (insert_proportion > 0)
        op_gen.AddValue(INSERT, insert_proportion);
    if (scan_proportion > 0)
        op_gen.AddValue(SCAN, scan_proportion);

    size_t record_count = std::stoull(p.GetProperty("recordcount"));
    insert_key_gen = std::make_unique<UniformGenerator>(0, record_count - 1);

    int64_t keyrange_num = std::stoll(p.GetProperty("keyrange_num", "1"));
    double prefix_a = std::stod(p.GetProperty("prefix_a"));
    double prefix_b = std::stod(p.GetProperty("prefix_b"));
    double prefix_c = std::stod(p.GetProperty("prefix_c"));
    double prefix_d = std::stod(p.GetProperty("prefix_d"));
    double key_dist_a = std::stod(p.GetProperty("key_dist_a"));
    double key_dist_b = std::stod(p.GetProperty("key_dist_b"));
    key_gen = std::make_unique<TwoTermExpGenerator>(0, record_count - 1, keyrange_num,
                                                    prefix_a, prefix_b, prefix_c, prefix_d,
                                                    key_dist_a, key_dist_b);
    
    double value_size_theta = std::stod(p.GetProperty("value_size_theta", "0"));
    double value_size_k = std::stod(p.GetProperty("value_size_k", "0.2615"));
    double value_size_sigma = std::stod(p.GetProperty("value_size_sigma", "25.45"));
    value_size_gen = std::make_unique<ParetoGenerator>(value_size_theta, value_size_k, value_size_sigma);
    
    double scan_len_theta = std::stod(p.GetProperty("scan_len_theta", "0"));
    double scan_len_k = std::stod(p.GetProperty("scan_len_k", "2.517"));
    double scan_len_sigma = std::stod(p.GetProperty("scan_len_sigma", "14.236"));
    scan_len_gen = std::make_unique<ParetoGenerator>(scan_len_theta, scan_len_k, scan_len_sigma);
    
    key_size = std::stoull(p.GetProperty("keysize", "16"));
    insert_value_size = std::stoull(p.GetProperty("insert_valuesize", "100"));
    valuesize_max = std::stoull(p.GetProperty("valuesize_max", "1024"));
    scanlen_max = std::stoull(p.GetProperty("scanlen_max", "10000"));
    
    table_name = p.GetProperty("table", "usertable");
}

bool ycsbc::MixGraph::DoInsert(DB &db) {
    std::string key = build_key(insert_key_gen->Next(), key_size);
    std::vector<DB::Field> values = build_values(insert_value_size);
    auto status = db.Insert(table_name, key, values);
    return status == DB::kOK;
}

bool ycsbc::MixGraph::DoTransaction(DB &db) {
    switch (op_gen.Next()) {
        case READ:
            return TransactionRead(db);
        case INSERT:
            return TransactionInsert(db);
        case SCAN:
            return TransactionScan(db);
        default:
            throw utils::Exception("Operation request is not recognized!");
    }
}

bool ycsbc::MixGraph::TransactionRead(DB &db) {
    std::string key = build_key(key_gen->Next(), key_size);
    std::vector<DB::Field> result;
    auto status = db.Read(table_name, key, nullptr, result);
    return status == DB::kOK;
}

bool ycsbc::MixGraph::TransactionInsert(DB &db) {
    std::string key = build_key(key_gen->Next(), key_size);
    uint64_t value_size = std::min(value_size_gen->Next(), valuesize_max);
    std::vector<DB::Field> values = build_values(value_size);
    auto status = db.Insert(table_name, key, values);
    return status == DB::kOK;
}

bool ycsbc::MixGraph::TransactionScan(DB &db) {
    std::string key = build_key(key_gen->Next(), key_size);
    size_t scan_len = std::min(scan_len_gen->Next(), scanlen_max);
    std::vector<std::vector<DB::Field>> result;
    auto status = db.Scan(table_name, key, scan_len, nullptr, result);
    return status == DB::kOK;
}

static ycsbc::Workload* NewMixGraph() {
    return new ycsbc::MixGraph();
}

const static bool registered = ycsbc::WorkloadFactory::RegisterWorkload("MixGraph", NewMixGraph);
