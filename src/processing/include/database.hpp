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

#include <SQLiteCpp.h>

#include "reader.hpp"
#include "types.hpp"

namespace fs = std::filesystem;

const std::regex sd_r_str = std::regex("(\")");
#define RAW_TEXT(FIELD) {#FIELD, ST_TEXT, [](const Comment& json, std::string* out){ *out = '"' + json.FIELD + '"'; }}
#define TEXT(FIELD) {#FIELD, ST_TEXT, [](const Comment& json, std::string* out){ *out = '"' + std::regex_replace(json.FIELD, sd_r_str, "$&$&") + '"'; }}
#define INT(FIELD) {#FIELD, ST_INT, [](const Comment& json, std::string* out){ *out = std::to_string(json.FIELD); }}
#define BOOL(FIELD) {#FIELD, ST_BOOL, [](const Comment& json, std::string* out){ *out = ((char) json.FIELD) + 48; }}

enum SchemaType { ST_TEXT, ST_INT, ST_BOOL };
using SchemaCallback = std::function<void(const Comment&, std::string*)>;

// notice that all strings are encoded as utf8, so we dont need to do any conversions
// https://github.com/ArthurHeitmann/arctic_shift/blob/bde0d2e8d41c0b6ade62ff79f69a77d633741482/file_content_explanations.md?plain=1#L12

struct SchemaDef {
    const std::string key;
    const SchemaType type;
    const SchemaCallback callback;

    SchemaDef(const std::string& key, const SchemaType t, const SchemaCallback& callback) : key(key), type(t), callback(callback) {}
};

struct Schema {
private:
    // cols is for table creating, head is for inserting
    // will be in same order
    std::string cols;
    std::string head;
public:
    const std::shared_ptr<std::vector<SchemaDef>> def;

    Schema(std::vector<SchemaDef>& def) : def(std::shared_ptr<std::vector<SchemaDef>>(new std::vector<SchemaDef>(def))) {
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

    const std::string& columns() const { return cols; }
    const std::string& columns_ins() const { return head; }
};

using Tables = std::vector<std::pair<std::string, Schema>>;
using s_Buffer = std::unordered_map<std::string, std::pair<std::stringstream, const std::string*>>;

class Database {
private:
    const std::shared_ptr<Tables> tables;
    const std::shared_ptr<SQLite::Database> db;

    void write(s_Buffer& buffer) {
        SQLite::Transaction t(*db);
        for (auto& table : buffer) {
            std::string s = table.second.first.str();
            if (s.size() == 0) continue;
            s.resize(s.size() - 1); // remove trailing comma

            std::string cmd = "INSERT INTO " + table.first + " (" + *table.second.second +  ") VALUES " + s + ";";
            db->exec(cmd);

            // clear stream
            table.second.first.str("");
        }
        t.commit();
    }
public:
    Database(const std::string& file, const std::string& backup, const Tables& tables, bool clear = false)
    : tables(std::shared_ptr<Tables>(new Tables(tables))),
      db(new SQLite::Database(file, SQLite::OPEN_CREATE | SQLite::OPEN_READWRITE))
    {
        fs::path fp = file;
        fs::path bp = backup;
        if (backup != "" && fs::exists(fp)) {
            if (bp.has_filename()) throw std::invalid_argument("Backup (" + backup + ") must be a directory.");
            fs::create_directory(bp);

            auto t = std::chrono::system_clock::now();
            bp /= ("backup_" + std::format("{:%d_%m_%H-%M-%S}", floor<std::chrono::seconds>(t)) + ".db");
            fs::copy_file(fp, bp);
        }

        if (clear) {
            SQLite::Transaction t(*db);
            db->exec("DROP TABLE IF EXISTS comments");
            t.commit();
            db->exec("VACUUM");
        }

        SQLite::Transaction t(*db);
        for (const auto& table : tables) {
            db->exec("CREATE TABLE IF NOT EXISTS " + table.first + " (" + table.second.columns() + ") STRICT;");
        }
        t.commit();
    }

    void read(std::string& file, int count = -1, int bufLen = 100000) {
        Reader reader(file, count);

        if (count != -1) bufLen = std::min(bufLen, count / 10);

        // { table : (values to write, col)}
        s_Buffer buffer;
        for (const auto& table : *tables) buffer[table.first] = {{}, &table.second.columns_ins()};

        int _count = 0;
        for (const auto& j : reader.decompress<Comment>(bufLen)) {
            for (const auto& table : *tables) {
                std::stringstream& ss = buffer[table.first].first;
                std::vector<SchemaDef>& def = *table.second.def;

                ss << "(";
                int i = 0;
                int m = def.size() - 1;
                for (const auto& entry : def) {
                    std::string out;
                    entry.callback(j, &out);
                    ss << out;

                    if (i++ != m) ss << ",";
                }
                ss << "),";
            }

            if (_count != 0 && _count % bufLen == 0) write(buffer);

            _count++;
            if (count != -1 && _count >= count) break;
        }

        write(buffer);
        reader.print_end();
    }
};

#endif