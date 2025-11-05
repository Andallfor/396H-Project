#ifndef CMSC_SUBMISSIONS_HPP
#define CMSC_SUBMISSIONS_HPP

#include <string>
#include <optional>
#include "common.hpp"
#include "glaze/glaze.hpp"

struct Submission {
    std::string selftext;
    std::string subreddit;
    std::string id;
    std::variant<std::string, double> created_utc;
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

#endif