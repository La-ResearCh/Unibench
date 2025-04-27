#ifndef YCSB_C_TWO_TERM_EXP_GENERATOR_H_
#define YCSB_C_TWO_TERM_EXP_GENERATOR_H_
#include <vector>

#include "generator.h"

namespace ycsbc {

class TwoTermExpGenerator : public Generator<uint64_t> {
    public:
        TwoTermExpGenerator(uint64_t min, uint64_t max, uint64_t keyrange_num,
                            double prefix_a, double prefix_b, double prefix_c, double prefix_d,
                            double key_dist_a, double key_dist_b);
        virtual ~TwoTermExpGenerator() = default;

        uint64_t Next() override;
        uint64_t Last() override {
            return last;
        }
    private:
        struct KeyrangeEntry {
            uint64_t p_start;
            uint64_t k_start;
            uint64_t k_num;
        };
        std::vector<KeyrangeEntry> keyranges;
        double a, b;
        std::mt19937_64 gen;
        uint64_t last;
};

}

#endif