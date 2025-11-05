// #define BENCHMARK_ENABLED

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
namespace fs = std::filesystem;
#include "wrapper.hpp"

#ifdef _WIN32
fs::path m_in = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/in/months";
fs::path s_in = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/in/subreddits";

fs::path m_out = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/finished/users";
fs::path s_out = "C:/Users/andallfor/Documents/GitHub/396H-Project/data/finished/subreddits";
#endif
#ifdef linux
fs::path m_in = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/in/months";
fs::path s_in = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/in/subreddits";

fs::path m_out = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/finished/users";
fs::path s_out = "/mnt/c/Users/andallfor/Documents/GitHub/396H-Project/data/finished/subreddits";
#endif

struct resolvedPath {
    std::string cmt;
    std::string sub;
    std::string db;
};

resolvedPath resolveMonth(const std::string& month) {
    fs::path cmt = m_in / ("RC_" + month + ".zst");
    fs::path sub = m_in / ("RS_" + month + ".zst");

    if (!fs::exists(cmt) || !fs::exists(sub)) throw std::runtime_error("Non-existant month " + month);

    return { cmt.string(), sub.string(), (m_out / (month + ".db")).string() };
}

resolvedPath resolveSubreddit(const std::string& subreddit) {
    fs::path cmt = s_in / (subreddit + "_comments.zst");
    fs::path sub = s_in / (subreddit + "_submissions.zst");

    if (!fs::exists(cmt) || !fs::exists(sub)) throw std::runtime_error("Non-existant subreddit " + subreddit);

    return { cmt.string(), sub.string(), (s_out / (subreddit + ".db")).string() };
}

int main(int argc, const char** argv) {
    std::vector<std::string> subreddits = {
        // "AITAH",
        // "antiwork",
        "changemyview",
        "ChapoTrapHouse",
        "Conservative",
        "democrats",
        "dsa",
        "IAmA",
        "KotakuInAction",
        "MGTOW",
        "offmychest",
        "politics",
        "The_Donald",
        "TheNewRight",
        "unpopularopinion"
    };

    // auto r = resolveMonth("2025-07");
    for (auto& subreddit : subreddits) {
        auto r = resolveSubreddit(subreddit);
        Wrapper wrapper(r.cmt, r.sub, r.db);
        wrapper.read();
        wrapper.sampleSubreddit();
    }

    // wrapper.sampleUsers();

    return 0;
}
