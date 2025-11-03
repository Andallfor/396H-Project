#ifndef CMSC_TYPES_H
#define CMSC_TYPES_H

#include <optional>
#include <variant>
#include <vector>
#include "glaze/glaze.hpp"
#include "ctre.hpp"

// https://stackoverflow.com/questions/70857562/is-this-good-enough-to-check-an-ascii-string
bool isAscii(const unsigned char& c) { return (c & 0x80) == 0; }

// this is set in main.cpp
bool s1_first[256] = {false};
bool s1_second[256] = {false};
bool s2_first[256];
bool s2_second[256] = {false};

// https://support.reddithelp.com/hc/en-us/articles/360043033952-Formatting-Guide
// markdown link, raw link (http:// or https://), # (headers), ^ or >
// note that we dont match for stuff like _ * since those cant be done with regex
// though a brief search shows that only a small percentage of text has them (maybe like 7%?)
auto markdown = ctre::search_all<"\\[(.*?)\\]\\(.*?\\)|https?:\\/\\/\\S*|(^|\\n)[#>]+ *|([^\\\\])\\^">;

template <typename T>
concept TRedditData = requires(T t) {
    // called before any processing
    t.reset();
    // called after processing, return determines if we write entry to database
    { t.valid() } -> std::convertible_to<bool>;
};

struct _Replacement {
    int pos; int len;
    std::string str;
};

bool sanitize(std::string& body, int& num_sentences) {
    if (body.size() == 0 || body == "[deleted]") return false;

    // you can get much better performance with re2 or hyperscan but those are a bit of a nightmare to install

    // filter by ascii and num sentences, which removes ~95% of text (so below regex doesnt need to be run on everything)
    num_sentences = 0;
    int len = body.size(); // we have i + 1 which will get \0
    for (int i = 0; i < len; i++) {
        unsigned char c = body[i];
        unsigned char cc = body[i + 1];

        if (!isAscii(c)) return false;

        // ((\.|\?|!)\s)|(\S| )(\n|$)
        // its not a perfect sentence matcher but generally close enough
        if ((s1_first[c] && s1_second[cc]) || (s2_first[c] && s2_second[cc])) {
            num_sentences++;
            i++;
            // before we skip we also need to make sure the skipped char is ascii
            if (!isAscii(cc)) return false;
        }
    }

    if (num_sentences < 5) return false;

    // remove disruptive markdown
    int offset = 0;
    std::vector<_Replacement> toRemove;
    for (auto match : markdown(body)) {
        toRemove.push_back({
            (int) (match.begin() - body.begin()),
            (int) match.size(),
            (match.get<1>().size()) ? match.get<1>().to_string() :
            (match.get<2>().size()) ? match.get<2>().to_string() :
            (match.get<3>().size()) ? match.get<3>().to_string() : "",
        });
    }

    for (auto& r : toRemove) {
        body.replace(r.pos - offset, r.len, r.str);
        offset += r.len - r.str.size();
    }

    // remove backslash
    // since erase is expensive and num_sentences filters out so much it might (maybe) be more efficient to separate the loops
    // (its also easier)
    for (auto it = body.begin(); it != body.end(); it++) {
        if (*it == '\\') {
            it = body.erase(it);
            if (it == body.end()) break;
        }
    }

    return true;
}

struct Comment {
    std::string body;
    std::string subreddit;
    std::string id;
    std::string parent_id;
    int created_utc;
    int score;
    std::optional<std::string> distinguished;
    
    std::string author;
    int num_sentences;

    // because we just write to the same struct we need to reset the optional ones
    void reset() { distinguished.reset(); }

    bool valid() {
        if (author == "AutoModerator") return false;
        return sanitize(body, num_sentences);
    }
};

template <> struct glz::meta<Comment> {
    static constexpr bool skip(const std::string_view key, const meta_context&) {
        return key == "num_sentences";
    }
};

struct Submission {
    std::string selftext;
    std::string subreddit;
    std::string id;
    int created_utc;
    int score;
    std::optional<std::string> distinguished;

    int num_sentences;

    void reset() { distinguished.reset(); }

    // do not need to remove automoderator from here
    bool valid() { return sanitize(selftext, num_sentences); }
};

template <> struct glz::meta<Submission> {
    static constexpr bool skip(const std::string_view key, const meta_context&) {
        return key == "num_sentences";
    }
};

// notice: we assume enums have < 10 elements

enum DistinguishedEnum {
    D_ERROR,
    D_USER,
    D_MOD,
    D_ADMIN
};

void getDistinguished(const std::optional<std::string>& in, std::string& out) {
    DistinguishedEnum d = D_ERROR;
    if (in.has_value()) {
        int ss = in->size();
        if (ss == 9) d = D_MOD;
        else if (ss == 5) d = D_ADMIN;
    } else d = D_USER;

    out = 48 + (char) d;
}

#endif