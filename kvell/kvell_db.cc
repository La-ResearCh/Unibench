#include <mutex>
#include <future>

#include "kvell_db.h"
#include "core/db_factory.h"

extern "C" {

#include "slabworker.h"
#include "items.h"

}

using payload_t = std::pair<std::promise<std::vector<ycsbc::DB::Field>>, const std::vector<std::string>*>;

static size_t ref_cnt = 0;
static std::mutex init_mutex;

static void SerializeRow(const std::vector<ycsbc::DB::Field> &values, std::string &data) {
    for (const ycsbc::DB::Field &field : values) {
        uint32_t len = field.name.size();
        data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
        data.append(field.name.data(), field.name.size());
        len = field.value.size();
        data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
        data.append(field.value.data(), field.value.size());
    }
}

static void DeserializeRowFilter(std::vector<ycsbc::DB::Field> &values, const char *p, const char *lim,
                                 const std::vector<std::string> &fields) {
    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    while (p != lim && filter_iter != fields.end()) {
        assert(p < lim);
        uint32_t len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field(p, static_cast<const size_t>(len));
        p += len;
        len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string value(p, static_cast<const size_t>(len));
        p += len;
        if (*filter_iter == field) {
            values.push_back({field, value});
            filter_iter++;
        }
    }
    assert(values.size() == fields.size());
}

static void DeserializeRowFilter(std::vector<ycsbc::DB::Field> &values, const std::string &data,
                                 const std::vector<std::string> &fields) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRowFilter(values, p, lim, fields);
}

static void DeserializeRow(std::vector<ycsbc::DB::Field> &values, const char *p, const char *lim) {
    while (p != lim) {
        assert(p < lim);
        uint32_t len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field(p, static_cast<const size_t>(len));
        p += len;
        len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string value(p, static_cast<const size_t>(len));
        p += len;
        values.push_back({field, value});
    }
}

static void DeserializeRow(std::vector<ycsbc::DB::Field> &values, const std::string &data) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRow(values, p, lim);
}

static char* build_item(uint64_t key, const std::string &value) {
    char *item = new char[sizeof(item_metadata) + sizeof(uint64_t) + value.size()];
    auto item_meta = reinterpret_cast<item_metadata*>(item);
    item_meta->key_size = sizeof(uint64_t);
    item_meta->value_size = value.size();
    *reinterpret_cast<uint64_t*>(item + sizeof(item_metadata)) = key;
    memcpy(item + sizeof(item_metadata) + sizeof(uint64_t), value.data(), value.size());
    return item;
}

static slab_callback* build_callback(uint64_t key, const std::string &value) {
    slab_callback *cb = new slab_callback;
    cb->payload = nullptr;
    cb->item = build_item(key, value);
    return cb;
}

static void free_callback(slab_callback *cb) {
    delete[] static_cast<char*>(cb->item);
    delete cb;
}

static void read_cb(slab_callback *cb, void *item) {
    auto payload = static_cast<payload_t*>(cb->param);
    auto item_meta = static_cast<item_metadata*>(item);
    std::vector<ycsbc::DB::Field> result;
    if (item && item_meta->key_size != -1) {
        char *value = static_cast<char*>(item) + sizeof(item_metadata) + item_meta->key_size;
        char *value_end = value + item_meta->value_size;
        if (payload->second)
            DeserializeRowFilter(result, value, value_end, *payload->second);
        else
            DeserializeRow(result, value, value_end);
    }
    payload->first.set_value(std::move(result));
    free_callback(cb);
}

static void put_cb(slab_callback *cb, void *item) {
    memory_index_add(cb, item);
    free_callback(cb);
}

static void delete_cb(slab_callback *cb, void *item) {
    free_callback(cb);
}

void ycsbc::KvellDB::Init() {
    std::lock_guard<std::mutex> lock(init_mutex);
    if (ref_cnt++ == 0) {
        DB_PATH = strdup(props_->GetProperty("kvell.dbname", "/scratch%lu/kvell").c_str());
        PAGE_CACHE_SIZE = std::stoull(props_->GetProperty("kvell.cache_size", "32212254720"));
        int nb_disks = std::stoi(props_->GetProperty("kvell.nb_disks", "1"));
        int nb_workers_per_disk = std::stoi(props_->GetProperty("kvell.nb_workers_per_disk", "16"));
        slab_workers_init(nb_disks, nb_workers_per_disk);
    }
}

void ycsbc::KvellDB::Cleanup() {
    std::lock_guard<std::mutex> lock(init_mutex);
    if (--ref_cnt == 0) {
        // TODO: cleanup slab workers
        ref_cnt = 1; // refuse cleanup at present
    }
}

ycsbc::DB::Status ycsbc::KvellDB::Read(const std::string &table, const std::string &key,
                                       const std::vector<std::string> *fields,
                                       std::vector<DB::Field> &result) {
    uint64_t key_num = atoll(key.c_str() + 4);
    slab_callback *cb = build_callback(key_num, "");
    payload_t payload;
    payload.second = fields;
    cb->cb = read_cb;
    cb->param = &payload;
    kv_read_async(cb);

    auto future = payload.first.get_future();
    result = future.get();
    if (result.empty())
        return DB::kNotFound;
    return DB::kOK;
}

ycsbc::DB::Status ycsbc::KvellDB::Scan(const std::string &table, const std::string &key,
                                       int record_count, const std::vector<std::string> *fields,
                                       std::vector<std::vector<DB::Field>> &result) {
    uint64_t key_num = atoll(key.c_str() + 4);
    std::unique_ptr<char[]> item(build_item(key_num, ""));
    auto scan_res = kv_init_scan(item.get(), record_count);
    item.reset();

    std::vector<payload_t> payloads(scan_res.nb_entries);
    for(size_t i = 0; i < scan_res.nb_entries; i++) {
        payloads[i].second = fields;
        auto cb = build_callback(scan_res.hashes[i], "");
        cb->cb = read_cb;
        cb->param = &payloads[i];
        kv_read_async_no_lookup(cb, scan_res.entries[i].slab, scan_res.entries[i].slab_idx);
    }
    free(scan_res.hashes);
    free(scan_res.entries);

    result.clear();
    result.reserve(scan_res.nb_entries);
    for (auto &payload : payloads) {
        auto future = payload.first.get_future();
        result.push_back(future.get());
    }
    return DB::kOK;
}

ycsbc::DB::Status ycsbc::KvellDB::Insert(const std::string &table, const std::string &key,
                                         std::vector<DB::Field> &values) {
    uint64_t key_num = atoll(key.c_str() + 4);
    std::string value;
    SerializeRow(values, value);
    auto cb = build_callback(key_num, value);
    cb->cb = put_cb;
    kv_add_or_update_async(cb);
    return DB::kOK;
}

ycsbc::DB::Status ycsbc::KvellDB::Update(const std::string &table, const std::string &key,
                                         std::vector<DB::Field> &values) {
    uint64_t key_num = atoll(key.c_str() + 4);
    payload_t payload;
    payload.second = nullptr;
    auto cb1 = build_callback(key_num, "");
    cb1->cb = read_cb;
    cb1->param = &payload;
    kv_read_async(cb1);

    auto future = payload.first.get_future();
    auto current_values = future.get();
    if (current_values.empty())
        return DB::kNotFound;

    for (Field &new_field : values) {
        bool found = false;
        for (Field &cur_field : current_values) {
            if (cur_field.name == new_field.name) {
                found = true;
                cur_field.value = new_field.value;
                break;
            }
        }
        assert(found);
    }

    std::string value;
    SerializeRow(current_values, value);
    auto cb2 = build_callback(key_num, value);
    cb2->cb = put_cb;
    kv_update_async(cb2);
    return DB::kOK;
}

ycsbc::DB::Status ycsbc::KvellDB::Delete(const std::string &table, const std::string &key) {
    uint64_t key_num = atoll(key.c_str() + 4);
    auto cb = build_callback(key_num, "");
    cb->cb = delete_cb;
    kv_remove_async(cb);
    return DB::kOK;
}

static ycsbc::DB* NewKvellDB() {
    return new ycsbc::KvellDB;
}

const static bool registered = ycsbc::DBFactory::RegisterDB("kvell", NewKvellDB);