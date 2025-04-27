#ifndef YCSB_C_MIXGRAPH_H_
#define YCSB_C_MIXGRAPH_H_
#include <vector>

#include "workload.h"
#include "discrete_generator.h"
#include "generator.h"

namespace ycsbc {

class MixGraph: public Workload {
    public:
        virtual ~MixGraph() = default;
        virtual void Init(const utils::Properties &p) override;
        virtual bool DoInsert(DB &db) override;
        virtual bool DoTransaction(DB &db) override;
    protected:
        bool TransactionRead(DB &db);
        bool TransactionInsert(DB &db);
        bool TransactionScan(DB &db);
    private:
        DiscreteGenerator<Operation> op_gen;
        std::unique_ptr<Generator<uint64_t>> insert_key_gen;
        std::unique_ptr<Generator<uint64_t>> key_gen;
        std::unique_ptr<Generator<uint64_t>> value_len_gen;
        std::unique_ptr<Generator<uint64_t>> scan_len_gen;

        size_t key_size;
        size_t insert_value_size;

        std::string table_name;
};

}

#endif