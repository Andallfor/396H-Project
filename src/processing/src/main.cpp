// #define BENCHMARK_ENABLED

#include <iostream>
#include <string>
#include <vector>
#include "database.hpp"
#include "reader.hpp"
#include "types.hpp"

int main(int argc, const char** argv) {
#ifdef _WIN32
    std::string p_data_comment = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/RC_2025-07.zst";
    std::string p_data_submission = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/RS_2025-07.zst";
    std::string p_db = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/database.db";
    std::string p_backup = "";
    // std::string p_backup = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/backup/";
#endif
#ifdef linux
    std::string p_data_comment = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/RC_2025-07.zst";
    std::string p_data_submission = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/RS_2025-07.zst";
    std::string p_db = "./database.db";
    std::string p_backup = "";
#endif
    // this is a bit scuffed but we want these to be global variables but we need to init them
    // so they are defined in types.hpp (and used there), but we initialize them here
    s1_first['.'] = true;
    s1_first['?'] = true;
    s1_first['!'] = true;
    s1_second['\0'] = true;
    s1_second['\r'] = true;
    s1_second['\n'] = true;
    s1_second['\t'] = true;
    s1_second['\f'] = true;
    s1_second['\v'] = true;
    s1_second[' '] = true;

    std::fill_n(s2_first, 256, true);
    s2_first['\r'] = false;
    s2_first['\n'] = false;
    s2_first['\t'] = false;
    s2_first['\f'] = false;
    s2_first['\v'] = false;
    s2_second['\n'] = true;
    s2_second['\0'] = true;

    Schema<Comment> ct = {"comments", {
        RAW_TEXT(body), // notice that we dont need to sanitize string since we write as a prepared statement
        RAW_TEXT(subreddit),
        RAW_TEXT(id),
        RAW_TEXT(parent_id),
        INT(created_utc),
        INT(score),
        {"num_sentences", ST_INT, [](const Comment& j, std::string& out) { out = std::to_string(j.num_sentences); }},
        {"distinguished", ST_INT, [](const Comment& j, std::string& out) { getDistinguished(j.distinguished, out); }},
    }};

    Database<Comment> db(p_db, p_backup, ct, true);
    size_t nc = db.read(p_data_comment, 0);
    db.~Database();

    Schema<Submission> st = {"submissions", {
        // submissions call body selftext, so rename here
        {"body", ST_TEXT, [](const Submission& j, std::string& out) { out = j.selftext; }},
        RAW_TEXT(subreddit),
        RAW_TEXT(id),
        {"parent_id", ST_TEXT, [](const Submission&, std::string& out) { out = ""; }},
        INT(created_utc),
        INT(score),
        {"num_sentences", ST_INT, [](const Submission& j, std::string& out) { out = std::to_string(j.num_sentences); }},
        {"distinguished", ST_INT, [](const Submission& j, std::string& out) { getDistinguished(j.distinguished, out); }},
    }};

    Database<Submission> db2(p_db, p_backup, st, true);
    size_t ns = db2.read(p_data_submission, 0);

    // randomly sample
    // https://stackoverflow.com/questions/4114940/select-random-rows
    db2.exec(std::format(
        "DROP TABLE IF EXISTS r_comments; \
        DROP TABLE IF EXISTS r_submissions; \
        CREATE TABLE r_comments AS SELECT * FROM comments WHERE rowid IN (SELECT rowid FROM comments ORDER BY RANDOM() LIMIT {}); \
        CREATE TABLE r_submissions AS SELECT * FROM submissions WHERE rowid IN (SELECT rowid FROM submissions ORDER BY RANDOM() LIMIT {}); \
        DROP TABLE comments; \
        DROP TABLE submissions; \
        VACUUM;",
        5000, 5000)
    );

    return 0;
}
