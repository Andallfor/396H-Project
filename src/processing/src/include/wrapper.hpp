#ifndef CMSC_WRAPPER_HPP
#define CMSC_WRAPPER_HPP

#include <string>
#include <filesystem>
#include <variant>
#include <chrono>
#include <cstdlib>

namespace fs = std::filesystem;

#include "database.hpp"
#include "common.hpp"
#include "comments.hpp"
#include "submissions.hpp"

class Wrapper {
    Database<Comment>* cmt;
    Database<Submission>* sub;

    size_t last = 0;

    std::string in_cmt;
    std::string in_sub;
public:
    Wrapper(std::string& comments, std::string& submissions, std::string& out) : in_cmt(comments), in_sub(submissions) {
        if (fs::exists(out)) fs::remove(out);

        // this is a bit scuffed but we want these to be global variables but we need to init them
        // so they are defined in common.hpp (and used there), but we initialize them here
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

        cmt = new Database<Comment>(out, {"main", {
            RAW_TEXT(body), // notice that we dont need to sanitize string since we write as a prepared statement
            RAW_TEXT(subreddit),
            RAW_TEXT(id),
            {"parent_id", ST_TEXT, [](const Comment& j, std::string& out) {
                const std::string* val = std::get_if<std::string>(&j.parent_id);
                if (val) out = *val;
                else out = std::to_string(std::get<double>(j.parent_id));
            }},
            {"created_utc", ST_INT, [&last = last](const Comment& j, std::string& out) {
                const std::string* val = std::get_if<std::string>(&j.created_utc);
                double d;
                if (val) {
                    d = atof(val->c_str());
                    out = *val;
                } else {
                    d = std::get<double>(j.created_utc);
                    out = std::to_string(d);
                }

                // mildly inefficient but oh well!
                if (d > last) last = d;
            }},
            INT(score),
            {"num_sentences", ST_INT, [](const Comment& j, std::string& out) { out = std::to_string(j.num_sentences); }},
            {"distinguished", ST_INT, [](const Comment& j, std::string& out) { getDistinguished(j.distinguished, out); }},
        }}, false);

        sub = new Database<Submission>(out, {"main", {
            // submissions call body selftext, so rename here
            {"body", ST_TEXT, [](const Submission& j, std::string& out) { out = j.selftext; }},
            RAW_TEXT(subreddit),
            RAW_TEXT(id),
            {"parent_id", ST_TEXT, [](const Submission&, std::string& out) { out = ""; }},
            {"created_utc", ST_INT, [&last = last](const Submission& j, std::string& out) {
                const std::string* val = std::get_if<std::string>(&j.created_utc);
                double d;
                if (val) {
                    d = atof(val->c_str());
                    out = *val;
                } else {
                    d = std::get<double>(j.created_utc);
                    out = std::to_string(d);
                }

                if (d > last) last = d;
            }},
            INT(score),
            {"num_sentences", ST_INT, [](const Submission& j, std::string& out) { out = std::to_string(j.num_sentences); }},
            {"distinguished", ST_INT, [](const Submission& j, std::string& out) { getDistinguished(j.distinguished, out); }},
        }}, false);
    }

    ~Wrapper() {
        delete cmt;
        delete sub;
        cmt = nullptr;
        sub = nullptr;
    }

    void read(int count = 0) {
        auto p1 = cmt->read(in_cmt, count, 50000, 5, false);
        auto p2 = sub->read(in_sub, count, 50000, 5, false);

        std::cout << std::format("{}: {}/{}\n", p1.fileName, p1.readLinesTotal, p1.readLinesTotal - p1.readLinesFiltered - p1.readLinesInvalid);
        std::cout << std::format("{}: {}/{}\n", p2.fileName, p2.readLinesTotal, p2.readLinesTotal - p2.readLinesFiltered - p2.readLinesInvalid);
    }

    void sampleUsers(unsigned int count = 5000, bool drop = true) {
        sub->exec(std::format(
            "DROP TABLE IF EXISTS r_users; \
            DROP TABLE IF EXISTS r_mods; \
            CREATE TABLE r_users AS SELECT * FROM main WHERE rowid IN \
                (SELECT rowid FROM main WHERE distinguished = 1 ORDER BY RANDOM() LIMIT {}); \
            CREATE TABLE r_mods AS SELECT * FROM main WHERE rowid IN \
                (SELECT rowid FROM main WHERE distinguished = 2 ORDER BY RANDOM() LIMIT {});",
            count, count
        ));

        if (drop) sub->exec("DROP TABLE main; VACUUM");
    }

    void sampleSubreddit(unsigned int count = 1000, bool drop = true) {
        sub->exec("DROP TABLE IF EXISTS r_subreddit; CREATE TABLE r_subreddit (" + cmt->getSchema() + ");");
        sub->exec("DROP TABLE IF EXISTS months; CREATE TABLE months (month TEXT, timestamp INTEGER)");

        std::vector<int64_t> months;
        int start = 2005, end = 2024;
        size_t first = last - (365 + 31) * 24 * 60 * 60;
        for (int year = start; year <= end; year++) {
            for (int month = 1; month <= 12; month++) {
                std::chrono::utc_clock::time_point tp;
                std::istringstream ss(std::format("1/{}/{}", month, year));
                ss >> std::chrono::parse("%d/%m/%Y", tp);

                int64_t t = std::chrono::floor<std::chrono::seconds>(tp).time_since_epoch().count();
                if ((size_t) t < first || (size_t) t > last) continue;

                sub->exec(std::format(
                    "INSERT INTO months (month, timestamp) VALUES (\"{}\", {});",
                    std::format("{:02}/{}", month, year), t));
                months.push_back(t);
            }
        }

        for (size_t i = 0; i < months.size(); i++) {
            int64_t start = months[i];

            std::string end = i == months.size() - 1 ? "" : std::format("AND created_utc < {}", months[i + 1]);
            std::string cmd = std::format(
                "INSERT INTO r_subreddit SELECT * FROM main WHERE rowid IN \
                    (SELECT rowid FROM main WHERE created_utc >= {} {} ORDER BY RANDOM() LIMIT {});",
                start, end, count);

            sub->exec(cmd);
        }

        if (drop) sub->exec("DROP TABLE main; VACUUM;");
    }
};

#endif