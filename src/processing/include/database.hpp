#ifndef CMSC_DATABASE_H
#define CMSC_DATABASE_H

#include <unordered_map>
#include <memory>
#include <string>
#include <filesystem>
#include <stdexcept>
#include <chrono>
#include <format>
#include <regex>
#include <generator>
#include <functional>
#include <format>

#include "sqlite3.h"

#include "reader.hpp"
#include "types.hpp"
#include "timing.hpp"

namespace fs = std::filesystem;

#define RAW_TEXT(FIELD) {#FIELD, ST_TEXT, [](const auto& json, std::string& out) { out = json.FIELD; }}
#define INT(FIELD) {#FIELD, ST_INT, [](const auto& json, std::string& out) { out = std::to_string(json.FIELD); }}
#define BOOL(FIELD) {#FIELD, ST_BOOL, [](const auto& json, std::string& out) { out = ((char) json.FIELD) + 48; }}

enum SchemaType { ST_TEXT, ST_INT, ST_BOOL };
using SchemaCallback = std::function<void(const Comment&, std::string&)>;

// notice that all strings are encoded as utf8, so we dont need to do any conversions
// https://github.com/ArthurHeitmann/arctic_shift/blob/bde0d2e8d41c0b6ade62ff79f69a77d633741482/file_content_explanations.md?plain=1#L12

struct SchemaDef {
    const std::string key;
    const SchemaType type;
    const SchemaCallback callback;

    SchemaDef(const std::string& key, const SchemaType type, const SchemaCallback& callback) : key(key), type(type), callback(callback) {}
    SchemaDef(const SchemaDef& a) : key(a.key), type(a.type), callback(a.callback) {}
    SchemaDef() = delete;
};

struct Schema {
private:
    // cols is for table creating, head is for inserting
    // will be in same order
    std::string cols;
    std::string head;

    void init() {
        std::stringstream _cols;
        std::stringstream _head;

        int i = 0;
        int m = def.size() - 1;
        for (const auto& s : def) {
            switch (s.type) {
                case ST_TEXT:
                    _cols << s.key + " TEXT";
                    break;
                case ST_INT:
                    _cols << s.key + " INTEGER";
                    break;
                case ST_BOOL:
                    _cols << s.key + " INTEGER CHECK (" << s.key << " IN (0,1))";
                    break;
            }

            _head << s.key;

            if (i++ != m) {
                _head << ",";
                _cols << ",";
            }
        }

        cols = _cols.str();
        head = _head.str();
    }
public:
    const std::string name;
    const std::vector<SchemaDef> def;

    Schema(const std::string& table, const std::vector<SchemaDef>& def) : name(table), def(def) { init(); }
    Schema(const Schema& a) : name(a.name), def(a.def) { init(); }
    Schema() = delete;

    const std::string& columns() const { return cols; }
    const std::string& columns_ins() const { return head; }
};

class Database {
private:
    const std::vector<Schema> tables;
    sqlite3* db;

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    void tryThrowSql(int ret, const std::string& msg, char* customErr = nullptr) const {
        if (ret != SQLITE_OK || customErr != nullptr) {
            std::string out = std::format("{}\n  SQLite3 error code {}: {}", msg, ret, sqlite3_errstr(ret));
            if (customErr != nullptr) {
                out += "\n  " + std::string(customErr);
                sqlite3_free(customErr);
            }

            throw std::runtime_error(out);
        }
    }

    void exec(const std::string& cmd, const std::string& errMsg) const {
        char* err = 0;
        int ret = sqlite3_exec(db, cmd.c_str(), nullptr, nullptr, &err);
        tryThrowSql(ret, errMsg, err);
    }

    void exec(const std::string& cmd) const { exec(cmd, "Could not run " + cmd); }
public:
    Database(const std::string& file, const std::string& backup, const std::vector<Schema>& tables, bool clear = false) : tables(tables) {
        fs::path fp = file;
        fs::path bp = backup;
        if (backup != "" && fs::exists(fp)) {
            if (bp.has_filename()) throw std::invalid_argument("Backup (" + backup + ") must be a directory.");
            fs::create_directory(bp);

            auto t = std::chrono::system_clock::now();
            bp /= ("backup_" + std::format("{:%d_%m_%H-%M-%S}", floor<std::chrono::seconds>(t)) + ".db");
            fs::copy_file(fp, bp);
        }

        // if previous run left a journal (writes that had not been committed) remove
        fs::path journal = file + "-journal";
        if (fs::exists(journal)) fs::remove(journal);

        int ret = sqlite3_open_v2(file.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr); 
        tryThrowSql(ret, "Could not open database " + file);

        exec("BEGIN TRANSACTION", "Unable to start transaction in database initialization");
        for (const auto& table : tables) {
            if (clear) exec("DROP TABLE IF EXISTS " + table.name, "Could not drop " + table.name);
            exec(std::format("CREATE TABLE IF NOT EXISTS {} ({}) STRICT;", table.name, table.columns()), "Unable to create table " + table.name);
        }
        exec("END TRANSACTION", "Unable to end transaction in database initialization");

        exec("PRAGMA page_size = 8192");
        exec("VACUUM");
    }

    ~Database() {
        int ret = sqlite3_close(db);
        tryThrowSql(ret, "Could not close database (are there any unfinished statements?)");
    }

    void read(const std::string& _table, const std::string& file, const size_t count = 0, int writeBuf = 50000, int insBuf = 5) {
        Reader reader(file, count);

        const Schema* table = nullptr;
        for (const auto& t : tables) {
            if (t.name == _table) {
                table = &t;
                break;
            }
        }

        if (table == nullptr) throw std::runtime_error("Unknown table name " + _table);

        if (count != 0) writeBuf = (int) std::min((size_t) writeBuf, count / 10);

        std::stringstream sbind;
        for (int j = 0; j < insBuf; j++) {
            sbind << "(";
            for (int i = 0; i < (int) table->def.size() - 1; i++) sbind << "?,";
            sbind << "?)";
            if (j != insBuf - 1) sbind << ",";
        }

        sqlite3_stmt* stmt;
        std::string stmtStr = std::format("INSERT INTO {} ({}) VALUES {}", table->name, table->columns_ins(), sbind.str());
        int ret = sqlite3_prepare_v3(db, stmtStr.c_str(), stmtStr.size() + 1, SQLITE_PREPARE_PERSISTENT, &stmt, nullptr);
        tryThrowSql(ret, "Could not create prepared statement: " + stmtStr);

        int e_len = table->def.size();
        int e_i = 0, ins_off = 0, ins_cnt = 0;
        size_t _count = 0;
        std::vector<std::string> writeBuffer(insBuf * e_len, "");

        const char* beginTrans = "BEGIN TRANSACTION";
        const char* endTrans = "END TRANSACTION";

        exec("BEGIN TRANSACTION");

        // note that we dont use exec(...) below for performance
        for (const auto& j : reader.decompress<Comment>(writeBuf)) {
#ifdef BENCHMARK_ENABLED
            auto t_process = Benchmark::timestamp();
#endif
            for (e_i = 0; e_i < e_len; e_i++) table->def[e_i].callback(j, writeBuffer[ins_cnt++]);
            _count++;
#ifdef BENCHMARK_ENABLED
            Benchmark::sum("Process", t_process);
#endif
            if (_count % insBuf == 0) {
#ifdef BENCHMARK_ENABLED
                auto t_sql = Benchmark::timestamp();
#endif
                // write here instead of in above loop where we call all the callbacks so we can individually measure the performance of process v sql
                for (e_i = 0; e_i < ins_cnt; e_i++) sqlite3_bind_text(stmt, e_i + 1, writeBuffer[e_i].c_str(), writeBuffer[e_i].size(), SQLITE_STATIC);

                if (sqlite3_step(stmt) != SQLITE_DONE) throw std::runtime_error("Could not step prepared statement: " + std::string(sqlite3_errmsg(db)));
                sqlite3_reset(stmt);
                ins_cnt = 0;
#ifdef BENCHMARK_ENABLED
                Benchmark::sum("SQL", t_sql);
#endif
            }

            if (_count % writeBuf == 0) {
#ifdef BENCHMARK_ENABLED
                auto t_sql = Benchmark::timestamp();
#endif
                sqlite3_exec(db, endTrans, nullptr, nullptr, nullptr);
                sqlite3_exec(db, beginTrans, nullptr, nullptr, nullptr);
#ifdef BENCHMARK_ENABLED
                Benchmark::sum("SQL", t_sql);
#endif
            }

            if (count != 0 && _count >= count) break;
        }

        if (ins_cnt != 0) {
            sqlite3_clear_bindings(stmt);
            for (e_i = 0; e_i < ins_cnt; e_i++) sqlite3_bind_text(stmt, e_i + 1, writeBuffer[e_i].c_str(), writeBuffer[e_i].size(), SQLITE_STATIC);
            sqlite3_step(stmt);
        }

        exec("END TRANSACTION");
        exec("PRAGMA optimize");

        ret = sqlite3_finalize(stmt);
        tryThrowSql(ret, "Could not close prepared statement");

        reader.print_end();
    }
};
#endif