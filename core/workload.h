#ifndef YCSB_C_WORKLOAD_H_
#define YCSB_C_WORKLOAD_H_
#include <string>
#include "db.h"

namespace ycsbc {

enum Operation {
    INSERT = 0,
    READ,
    UPDATE,
    SCAN,
    READMODIFYWRITE,
    DELETE,
    INSERT_FAILED,
    READ_FAILED,
    UPDATE_FAILED,
    SCAN_FAILED,
    READMODIFYWRITE_FAILED,
    DELETE_FAILED,
    MAXOPTYPE
};

extern const char *kOperationString[MAXOPTYPE];

class Workload {
    public:
        static const std::string INSERT_START_PROPERTY;
        static const std::string INSERT_START_DEFAULT;

        virtual ~Workload() = default;

        virtual void Init(const utils::Properties &p) = 0;
      
        virtual bool DoInsert(DB &db) = 0;
        virtual bool DoTransaction(DB &db) = 0;
};

}
#endif