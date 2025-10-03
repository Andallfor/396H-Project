from enum import Enum
from abc import ABC, abstractmethod

# ERROR means we encountered an unexpected value

class RemovalType(Enum):
    none = 0 # not removed
    deleted = 1 # removed by poster
    removed = 2 # removed by moderator
    reddit = 3 # removed by reddit
    legal = 4 # see removal_reason

    ERROR = -1

    @staticmethod
    def make(obj):
        if obj['removal_reason']:
            return RemovalType.legal

        if '_meta' not in obj or 'removal_type' not in obj['_meta']:
            return RemovalType.none
        
        match obj['_meta']['removal_type']:
            case 'deleted':
                return RemovalType.deleted
            case 'removed':
                return RemovalType.removed
            case 'removed by reddit':
                return RemovalType.reddit
            case _:
                return RemovalType.ERROR

class Collapsed(Enum):
    none = 0
    score = 1 # collapsed_reason == "comment score below threshold"
    deleted = 2 # collapsed_reason_code
    unknown = 3 # collapsed == true, but collapsed_reason and collapsed_reason_code == null
    # note that the collapsed_because_crowd_control does not seem to be in use anymore

    ERROR = -1

    @staticmethod
    def make(obj):
        if obj['collapsed_reason'] or obj['collapsed_reason_code'] == 'LOW_SCORE':
            return Collapsed.score
        
        if obj['collapsed_reason_code'] == 'DELETED':
            return Collapsed.deleted

        if obj['collapsed']:
            return Collapsed.unknown

        if obj['collapsed_reason_code'] or obj['collapsed_reason']:
            return Collapsed.ERROR
        
        return Collapsed.none

class Distinguished(Enum):
    user = 0
    moderator = 1
    admin = 2

    ERROR = -1

    @staticmethod
    def make(obj):
        match obj['distinguished']:
            case None:
                return Distinguished.user
            case 'moderator':
                return Distinguished.moderator
            case 'admin':
                return Distinguished.admin
            case _:
                return Distinguished.ERROR


class SubredditType(Enum):
    public = 0
    restricted = 1
    user = 2
    archived = 3

    ERROR = -1

    @staticmethod
    def make(obj):
        match obj['subreddit_type']:
            case 'public':
                return SubredditType.public
            case 'restricted':
                return SubredditType.restricted
            case 'user':
                return SubredditType.user
            case 'archived':
                return SubredditType.archived
            case _:
                return SubredditType.ERROR
