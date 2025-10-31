#ifndef CMSC_TYPES_H
#define CMSC_TYPES_H

#include <optional>
#include <variant>
#include <glaze/glaze.hpp>

/*
TODO: can remove some extraneous values from removal and collapsed
see the json month definitions
*/

template <typename T>
concept TRedditData = requires(T t) { t.reset(); };

struct _CommentMeta {
    std::optional<bool> is_edited;
    std::optional<std::string> removal_type;
};

struct Comment {
    std::optional<_CommentMeta> _meta;

    // look into using string_view (will need to keep json alive)
    // or disabling json escaping

    std::string author;
    std::string body;
    std::string permalink;

    std::string id;
    std::string link_id;
    std::string parent_id;

    std::string subreddit;
    std::string subreddit_id;
    std::string subreddit_type;

    bool archived;
    int created_utc;
    int controversiality;
    int score;
    bool is_submitter;
    bool locked;
    bool stickied;

    std::variant<bool, int> edited;
    std::optional<std::string> removal_reason;
    bool collapsed;
    std::optional<std::string> collapsed_reason;
    std::optional<std::string> collapsed_reason_code;
    std::optional<std::string> distinguished;

    // because we just write to the same struct we need to reset the optional ones
    void reset() {
        removal_reason.reset();
        _meta.reset();
        collapsed_reason.reset();
        collapsed_reason_code.reset();
        distinguished.reset();
    }
};

struct _SubmissionMeta {
    std::optional<std::string> removal_type;
};

struct Submission {
    std::string selftext;
    std::string author;
    std::string id;

    int score;
    int created_utc;

    std::string subreddit;
    std::string subreddit_id;
    std::string subreddit_type;

    std::string permalink;

    std::optional<_SubmissionMeta> _meta;
    std::optional<std::string> distinguished;

    void reset() {
        _meta.reset();
        distinguished.reset();
    }
};

// notice: we assume enums have < 10 elements

enum RemovalSubmissionEnum {
    RS_ERROR,
    RS_NONE,
    RS_DELETED,
    RS_MOD,
    RS_REDDIT,
    RS_AUTOMOD,
    RS_AUTHOR,
    RS_TAKEDOWN, // content or copyright takedown
    RS_OPS // community or "anti evil" ops
};

enum RemovalEnum {
    R_ERROR,
    R_NONE,
    R_DELETED,
    R_REMOVED,
    R_REDDIT,
    R_LEGAL,
};

enum CollapsedEnum {
    C_ERROR,
    C_NONE,
    C_SCORE,
    C_DELETED,
    C_UNKNOWN
};

enum DistinguishedEnum {
    D_ERROR,
    D_USER,
    D_MOD,
    D_ADMIN
};

enum SubredditEnum {
    S_ERROR,
    S_PUBLIC,
    S_RESTRICTED,
    S_USER,
    S_ARCHIVED
};

#endif