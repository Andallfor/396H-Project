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

#include <SQLiteCpp.h>

#include "reader.hpp"
#include "types.hpp"
#include "timing.hpp"

namespace fs = std::filesystem;

#define RAW_TEXT(FIELD) {#FIELD, ST_TEXT, [](const Comment& json, std::string& out) { out = json.FIELD; }}
#define INT(FIELD) {#FIELD, ST_INT, [](const Comment& json, std::string& out) { out = std::to_string(json.FIELD); }}
#define BOOL(FIELD) {#FIELD, ST_BOOL, [](const Comment& json, std::string& out) { out = ((char) json.FIELD) + 48; }}

enum SchemaType { ST_TEXT, ST_INT, ST_BOOL };
using SchemaCallback = std::function<void(const Comment&, std::string&)>;

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

        // if previous run left a journal (writes that had not been committed) remove
        fs::path journal = file + "-journal";
        if (fs::exists(journal)) fs::remove(journal);

        if (clear) {
            SQLite::Transaction t(*db);
            db->exec("DROP TABLE IF EXISTS comments");
            t.commit();
            db->exec("VACUUM");
        }

        SQLite::Transaction t(*db);
        for (const auto& table : tables) {
            db->exec(std::format("CREATE TABLE IF NOT EXISTS {} ({}) STRICT;", table.first, table.second.columns()));
        }
        t.commit();
    }

    void read(std::string& file, int count = -1, int writeBuf = 50000, int insBuf = 5) {
        Reader reader(file, count);

        if (count != -1) writeBuf = std::min(writeBuf, count / 10);

        std::unordered_map<std::string, std::unique_ptr<SQLite::Statement>> statements;
        std::unordered_map<std::string, int> insBufCount;
        for (const auto& table : *tables) {
            std::string binds = "";

            for (int j = 0; j < insBuf; j++) {
                binds += "(";
                for (int i = 0; i < (int) table.second.def->size() - 1; i++) binds += "?,";
                binds += "?),";
            }

            binds.resize(binds.size() - 1);

            // for some reason compiler yells at me unless i do this
            statements[table.first] = std::unique_ptr<SQLite::Statement>(
                new SQLite::Statement{*db, std::format("INSERT INTO {} ({}) VALUES {}", table.first, table.second.columns_ins(), binds)});
            
            insBufCount[table.first] = 1;
        }

        // sqlitecpp transactions dont really like constantly being juggled so just do it manually
        db->exec("BEGIN TRANSACTION");

        int _count = 0;
        for (const auto& j : reader.decompress<Comment>(writeBuf)) {
#ifdef BENCHMARK_ENABLED
            auto t_process = Benchmark::timestamp();
#endif
            for (const auto& table : *tables) {
                std::vector<SchemaDef>& def = *table.second.def;

                for (const auto& entry : def) {
                    std::string out;
                    entry.callback(j, out);

                    statements[table.first]->bind(insBufCount[table.first]++, out);
                }
            }

            if (_count != 0 && (_count + 1) % insBuf == 0) {
                for (const auto& table : *tables) {
                    statements[table.first]->exec();
                    statements[table.first]->clearBindings();
                    statements[table.first]->reset();
                    insBufCount[table.first] = 1;
                }
            }
#ifdef BENCHMARK_ENABLED
            Benchmark::sum("Process", t_process);
#endif
            if (_count != 0 && _count % writeBuf == 0) {
                db->exec("END TRANSACTION");
                db->exec("BEGIN TRANSACTION");
            }

            _count++;
            if (count != -1 && _count >= count) break;
        }

        db->exec("END TRANSACTION");
        reader.print_end();
    }
};

#endif