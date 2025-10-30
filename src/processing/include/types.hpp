#ifndef CMSC_TYPES_H
#define CMSC_TYPES_H

#include <optional>
#include <variant>
#include <glaze/glaze.hpp>

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
};

// notice: we assume enums have < 10 elements

enum RemovalEnum {
    R_ERROR,
    R_NONE,
    R_DELETED,
    R_REMOVED,
    R_REDDIT,
    R_LEGAL
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