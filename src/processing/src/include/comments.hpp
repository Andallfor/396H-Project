#ifndef CMSC_COMMENTS_HPP
#define CMSC_COMMENTS_HPP

#include <string>
#include <optional>
#include <variant>
#include "types.hpp"
#include "common.hpp"
#include "glaze/glaze.hpp"

struct Comment {
    std::string body;
    std::string subreddit;
    std::string id;
    std::variant<std::string, double> parent_id;
    std::variant<std::string, double> created_utc;
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

#endif