#ifndef YCSB_C_PARETO_GENERATOR_H_
#define YCSB_C_PARETO_GENERATOR_H_
#include <random>

#include "generator.h"

namespace ycsbc {

class ParetoGenerator : public Generator<uint64_t> {
    public:
        ParetoGenerator(double theta, double k, double sigma) {
            m_theta = theta;
            m_k = k;
            m_sigma = sigma;
        }
        virtual ~ParetoGenerator() = default;

        uint64_t Next() override {
            last = ceil(m_theta + m_sigma * (pow(dist(gen), -m_k) - 1) / m_k);
            return last;
        }
        uint64_t Last() override { return last; }

    private:
        std::mt19937_64 gen;
        std::uniform_real_distribution<double> dist;
        double m_theta;
        double m_k;
        double m_sigma;

        uint64_t last;
};

}

#endif