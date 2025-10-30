// #define BENCHMARK_ENABLED

#include <iostream>
#include <string>
#include <vector>
#include "database.hpp"
#include "reader.hpp"
#include "types.hpp"

int main(int argc, const char** argv) {
#ifdef _WIN32
    std::string p_data = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/RC_2025-07.zst";
    std::string p_db = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/database.db";
    std::string p_backup = "";
    // std::string p_backup = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/backup/";
#endif
#ifdef linux
    std::string p_data = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/RC_2025-07.zst";
    std::string p_db = "./database.db";
    std::string p_backup = "";
#endif
    bool s_first[256] = {false};
    s_first['.'] = true;
    s_first['?'] = true;
    s_first['!'] = true;
    bool s_whitespace[256] = {false};
    s_whitespace['\r'] = true;
    s_whitespace['\n'] = true;
    s_whitespace['\t'] = true;
    s_whitespace['\f'] = true;
    s_whitespace['\v'] = true;
    s_whitespace[' '] = true;

    std::vector<Schema> t = {
        {"comments", {
            RAW_TEXT(author),
            RAW_TEXT(id),
            RAW_TEXT(link_id),
            RAW_TEXT(parent_id),
            RAW_TEXT(subreddit_id),
            RAW_TEXT(subreddit),
            RAW_TEXT(permalink),
            INT(created_utc),
            INT(controversiality),
            INT(score),
            BOOL(archived),
            BOOL(locked),
            BOOL(is_submitter),
            BOOL(stickied),
            RAW_TEXT(body), // notice that we dont need to sanitize string since we write as a prepared statement
            {"num_sentences", ST_INT, [&s_first, &s_whitespace](const Comment& j, std::string& out) {
                // this is equivalent to ((\.|\?|!)\s)|(\S| )\n
                // its not a perfect sentence matcher but generally close enough
                // (importing a whole regex library for this seems overkill, so im just implementing it by hand)

                // we pad end of string with new line so we dont need to worry about over reading string
                std::string cp = j.body + "\n";

                bool prev = false;
                int len = j.body.size();
                int count = 0;
                for (int i = 0; i < len; i++) {
                    unsigned char c = cp[i];
                    unsigned char cc = cp[i + 1];
                    // notice that we assume no \r\n
                    if ((s_first[c] && s_whitespace[cc]) || ((!s_whitespace[c] || c == ' ') && cc == '\n')) {
                        count++;
                        i++;
                    }
                }

                out = std::to_string(count);
            }},
            {"edited", ST_BOOL, [](const Comment& j, std::string& out) {
                // edited is either false or the timestamp it was edited
                if (std::holds_alternative<bool>(j.edited)) out = "0";
                else out = "1";
            }},
            {"removal_type", ST_INT, [](const Comment& j, std::string& out) {
                RemovalEnum r = R_ERROR;

                if (j.removal_reason.has_value()) r = R_LEGAL;
                else if (!j._meta.has_value() || !(*j._meta).removal_type.has_value()) r = R_NONE;
                else {
                    const std::string& s = *(*j._meta).removal_type;
                    r = (s == "deleted") ? R_DELETED
                    : (s == "removed") ? R_REMOVED
                    : (s == "removed by reddit") ? R_REDDIT
                    : R_ERROR;
                }

                out = 48 + (char) r;
            }},
            {"collapsed", ST_INT, [](const Comment& j, std::string& out) {
                CollapsedEnum c = C_ERROR;

                if (!j.collapsed) c = C_NONE;
                else {
                    if (j.collapsed_reason.has_value()) c = C_SCORE;
                    else if (j.collapsed_reason_code.has_value()) {
                        const std::string& s = *j.collapsed_reason_code;
                        if (s == "DELETED") c = C_DELETED;
                        else if (s == "LOW_SCORE") c = C_SCORE;
                    }
                }

                out = 48 + (char) c;
            }},
            {"distinguished", ST_INT, [](const Comment& j, std::string& out) {
                DistinguishedEnum d = D_ERROR;

                if (j.distinguished.has_value()) {
                    const std::string& s = *j.distinguished;
                    if (s == "moderator") d = D_MOD;
                    else if (s == "admin") d = D_ADMIN;
                } else d = D_USER;

                out = 48 + (char) d;
            }},
            {"subreddit_type", ST_INT, [](const Comment& j, std::string& out) {
                SubredditEnum r = S_ERROR;

                const std::string& s = j.subreddit_type;

                if (s == "public") r = S_PUBLIC;
                else if (s == "restricted") r = S_RESTRICTED;
                else if (s == "user") r = S_USER;
                else if (s == "archived") r = S_ARCHIVED;

                out = 48 + (char) r;
            }}
        }}
    };

    Database db(p_db, p_backup, t, true);
    db.read("comments", p_data, 5000000);

    return 0;
}
