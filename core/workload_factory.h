#ifndef YCSB_C_WORKLOAD_FACTORY_H_
#define YCSB_C_WORKLOAD_FACTORY_H_
#include "workload.h"

namespace ycsbc {

class WorkloadFactory {
    public:
        using WorkloadCreator = Workload* (*)();
        static bool RegisterWorkload(std::string db_name, WorkloadCreator db_creator);
        static Workload *CreateWorkload(utils::Properties *props);
    private:
        static std::map<std::string, WorkloadCreator> &Registry();
};

}
#endif