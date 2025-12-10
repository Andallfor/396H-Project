#!/bin/bash

python create_bar.py --graph line \
--y-name "Mean Authoritarian Score" \
--y-size 3 \
--x-size 5 \
--bar "r/TheNewRight:TheNewRight_r_subreddit_per_text.csv" \
--bar "r/The_Donald:The_Donald_r_subreddit_per_text.csv" \
--bar "r/ChapoTrapHouse:ChapoTrapHouse_r_subreddit_per_text.csv"
