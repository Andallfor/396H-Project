# Useful Links
- [Pushshift dumps](https://academictorrents.com/browse.php?search=RaiderBDev)
- [Dumps by subreddit](https://academictorrents.com/details/1614740ac8c94505e4ecb9d88be8bed7b6afddd4)
- [.zst file schemas](https://github.com/ArthurHeitmann/arctic_shift/tree/master/schemas)
- Limited schema field descriptions: [comments](https://praw.readthedocs.io/en/stable/code_overview/models/comment.html), [submissions](https://praw.readthedocs.io/en/stable/code_overview/models/submission.html), [from official paper](https://ojs.aaai.org/index.php/ICWSM/article/download/7347/7201?utm_source=consensus)
    - Note that although this is a different library, they should roughly describe the same thing as they both pull from the Reddit API

# Other
- For .zst files, RC_* are comments, RS_* are submissions (posts)
- You may notice that some ids have `t(integer)_` prefixed. See the [type prefixes header](https://www.reddit.com/dev/api/) for the meaning.

# Issues
- [Users can choose what posts/comments appear on their profile](https://www.reddit.com/r/reddit/comments/1l2hl4l/curate_your_reddit_profile_content_with_new/)
- Some of the data fields are mysterious; some of the fields appear to be internal things for Reddit and are not fully documented ([source](https://www.reddit.com/r/pushshift/comments/1itme1k/comment/mevifds/))
- Schemas differ over time