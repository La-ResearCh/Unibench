#include "workload_factory.h"

bool ycsbc::WorkloadFactory::RegisterWorkload(std::string db_name, WorkloadCreator db_creator) {
    Registry()[db_name] = db_creator;
    return true;
}

ycsbc::Workload* ycsbc::WorkloadFactory::CreateWorkload(utils::Properties *props) {
    std::string workload_name = props->GetProperty("workload", "CoreWorkload");
    auto &registry = Registry();
    auto it = registry.find(workload_name);
    Workload *workload = nullptr;
    if (it != registry.end()) {
        workload = it->second();
        if (workload)
            workload->Init(*props);
    }
    return workload;
}

std::map<std::string, ycsbc::WorkloadFactory::WorkloadCreator>& ycsbc::WorkloadFactory::Registry() {
    static std::map<std::string, WorkloadCreator> registry;
    return registry;
}