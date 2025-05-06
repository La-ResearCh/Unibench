#ifndef YCSB_C_KVELL_DB_H_
#define YCSB_C_KVELL_DB_H_
#include "core/db.h"

namespace ycsbc {

class KvellDB : public DB {
    public:
        virtual ~KvellDB() = default;

        virtual void Init() override;
        virtual void Cleanup() override;
        virtual Status Read(const std::string &table, const std::string &key,
                            const std::vector<std::string> *fields,
                            std::vector<Field> &result) override;
        virtual Status Scan(const std::string &table, const std::string &key,
                            int record_count, const std::vector<std::string> *fields,
                            std::vector<std::vector<Field>> &result) override;
        virtual Status Insert(const std::string &table, const std::string &key,
                              std::vector<Field> &values) override;
        virtual Status Update(const std::string &table, const std::string &key,
                              std::vector<Field> &values) override;
        virtual Status Delete(const std::string &table, const std::string &key) override;
    private:
};

}

#endif