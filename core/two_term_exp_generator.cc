#include <random>
#include <algorithm>
#include <cmath>
#include <cassert>

#include "two_term_exp_generator.h"

ycsbc::TwoTermExpGenerator::TwoTermExpGenerator(uint64_t min, uint64_t max, uint64_t keyrange_num,
                                                double prefix_a, double prefix_b, double prefix_c, double prefix_d,
                                                double key_dist_a, double key_dist_b) {
    a_rec = 1.0 / key_dist_a;
    b_rec = 1.0 / key_dist_b;
    
    std::vector<double> weights;
    weights.reserve(keyrange_num);

    double sum = 0;
    for (int i = 1; i <= keyrange_num; i++) {
        double w = prefix_a * exp(prefix_b * i) + prefix_c * exp(prefix_d * i);
        weights.push_back(w);
        sum += w;
    }
    std::shuffle(weights.begin(), weights.end(), std::mt19937_64(*(uint64_t*)&sum));

    uint64_t p_start = 0;
    uint64_t k_start = min;
    uint64_t k_num = (max - min + 1) / keyrange_num;
    keyranges.resize(keyrange_num);
    for (int i = 0; i < keyrange_num; i++) {
        keyranges[i].p_start = p_start;
        keyranges[i].k_start = k_start;
        keyranges[i].k_num = k_num;
        p_start += weights[i] / sum * 1e64;
        k_start += k_num;
        assert(p_start >= keyranges[i].p_start);
    }
    keyranges.back().k_num += (max - min + 1) % keyrange_num;
}

uint64_t ycsbc::TwoTermExpGenerator::Next() {
    auto iter = std::upper_bound(keyranges.begin(), keyranges.end(), gen(),
        [](uint64_t val, const KeyrangeEntry& entry) {
        return val < entry.p_start;
    }) - 1;

    double u = std::uniform_real_distribution<double>(0, 1)(gen);
    uint64_t seed = pow(u * a_rec, b_rec);
    std::mt19937_64 tmpgen(seed);
    last = std::uniform_int_distribution<uint64_t>(iter->k_start, iter->k_start + iter->k_num - 1)(tmpgen);
    return last;
}
