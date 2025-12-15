# The Usage of Authoritarian Language in Reddit
This is an analysis on the usage of authoritarian language in Reddit developed for UMD's CMSC 396H class. The paper describing the work can be found [here](https://github.com/Andallfor/396H-Project/blob/main/papers/final/final.pdf). This repository serves as the host of the source code for the specially developed programs described in the pipeline within the paper.

# Structure
- [/src/processing](https://github.com/Andallfor/396H-Project/tree/main/src/processing): The data ingestion module; reads raw data from Pushshift .zst dumps and filters and transforms it into an SQLite database for use in the classifier.
- [/src/classifier](https://github.com/Andallfor/396H-Project/tree/main/src/classifier): Various scripts used to classify the data.
- [/src/python](https://github.com/Andallfor/396H-Project/tree/main/src/python): Old version of the data ingestion module written in Python. It is recommended to use the module in `/src/processing` as this version is significantly slower.

# Useful Links
- [Pushshift dumps](https://academictorrents.com/browse.php?search=RaiderBDev)
- [Dumps by subreddit](https://academictorrents.com/details/1614740ac8c94505e4ecb9d88be8bed7b6afddd4)
- [.zst file schemas](https://github.com/ArthurHeitmann/arctic_shift/tree/master/schemas)
- Limited schema field descriptions: [comments](https://praw.readthedocs.io/en/stable/code_overview/models/comment.html), [submissions](https://praw.readthedocs.io/en/stable/code_overview/models/submission.html), [from official paper](https://ojs.aaai.org/index.php/ICWSM/article/download/7347/7201?utm_source=consensus)
    - Note that although this is a different library, they should roughly describe the same thing as they both pull from the Reddit API

# Other
- For .zst files, RC_* are comments, RS_* are submissions (posts)
- You may notice that some ids have `t(integer)_` prefixed. See the [type prefixes header](https://www.reddit.com/dev/api/) for the meaning.
