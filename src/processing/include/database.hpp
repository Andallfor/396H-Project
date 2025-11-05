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

#include "types.hpp"
#include "reader.hpp"
#include "timing.hpp"

namespace fs = std::filesystem;

#define RAW_TEXT(FIELD) {#FIELD, ST_TEXT, [](const auto& json, std::string& out) { out = json.FIELD; }}
#define INT(FIELD) {#FIELD, ST_INT, [](const auto& json, std::string& out) { out = std::to_string((int) json.FIELD); }}
#define BOOL(FIELD) {#FIELD, ST_BOOL, [](const auto& json, std::string& out) { out = ((char) json.FIELD) + 48; }}

// notice that all strings are encoded as utf8, so we dont need to do any conversions
// https://github.com/ArthurHeitmann/arctic_shift/blob/bde0d2e8d41c0b6ade62ff79f69a77d633741482/file_content_explanations.md?plain=1#L12

template <TRedditData T>
class Database {
private:
    const Schema<T> table;
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
public:
    Database(const std::string& file, const Schema<T>& table, bool clear = false) : table(table) {
        int ret = sqlite3_open_v2(file.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr); 
        tryThrowSql(ret, "Could not open database " + file);

        exec("BEGIN TRANSACTION", "Unable to start transaction in database initialization");
        if (clear) exec("DROP TABLE IF EXISTS " + table.name, "Could not drop " + table.name);
        exec(std::format("CREATE TABLE IF NOT EXISTS {} ({}) STRICT;", table.name, table.columns()), "Unable to create table " + table.name);
        exec("END TRANSACTION", "Unable to end transaction in database initialization");

        exec("PRAGMA page_size = 8192");
        exec("VACUUM");
    }

    ~Database() {
        if (!db) return;
        int ret = sqlite3_close(db);
        tryThrowSql(ret, "Could not close database (are there any unfinished statements?)");

        db = nullptr;
    }

    void exec(const std::string& cmd) const { exec(cmd, "Could not run " + cmd); }
    void exec(const std::string& cmd, const std::string& errMsg) const {
        char* err = 0;
        int ret = sqlite3_exec(db, cmd.c_str(), nullptr, nullptr, &err);
        tryThrowSql(ret, errMsg, err);
    }

    const std::string& getSchema() const { return table.columns(); }

    const Reader_Output read(const std::string& file, const size_t count = 0, int writeBuf = 50000, int insBuf = 5, bool exitOnErr = true) {
        Reader reader(file, count);

        if (count != 0) writeBuf = (int) std::min((size_t) writeBuf, count / 10);

        std::stringstream sbind;
        for (int j = 0; j < insBuf; j++) {
            sbind << "(";
            for (int i = 0; i < (int) table.def.size() - 1; i++) sbind << "?,";
            sbind << "?)";
            if (j != insBuf - 1) sbind << ",";
        }

        sqlite3_stmt* stmt;
        std::string stmtStr = std::format("INSERT INTO {} ({}) VALUES {}", table.name, table.columns_ins(), sbind.str());
        int ret = sqlite3_prepare_v3(db, stmtStr.c_str(), stmtStr.size() + 1, SQLITE_PREPARE_PERSISTENT, &stmt, nullptr);
        tryThrowSql(ret, "Could not create prepared statement: " + stmtStr);

        int e_len = table.def.size();
        int e_i = 0, ins_off = 0, ins_cnt = 0;
        size_t _count = 0;
        std::vector<std::string> writeBuffer(insBuf * e_len, "");

        const char* beginTrans = "BEGIN TRANSACTION";
        const char* endTrans = "END TRANSACTION";

        exec("BEGIN TRANSACTION");

        // note that we dont use exec(...) below for performance
        for (const auto& j : reader.decompress<T>(writeBuf, count, exitOnErr)) {
#ifdef BENCHMARK_ENABLED
            auto t_process = Benchmark::timestamp();
#endif
            for (e_i = 0; e_i < e_len; e_i++) table.def[e_i].callback(j, writeBuffer[ins_cnt++]);
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

        return reader.status();
    }
};
#endif