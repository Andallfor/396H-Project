#!/bin/bash
python create_bar.py --graph scatter \
--x-col score \
--y-name "Authoritarian Score" \
--title "User Authoritarian Score Versus Post Score" \
--y-size 5.5 \
--x-size 5.5 \
--bar "Global:2024-09_r_users_per_text.csv,2024-10_r_users_per_text.csv,2024-11_r_users_per_text.csv,2024-12_r_users_per_text.csv,2025-01_r_users_per_text.csv,2025-02_r_users_per_text.csv,2025-03_r_users_per_text.csv,2025-04_r_users_per_text.csv,2025-05_r_users_per_text.csv,2025-06_r_users_per_text.csv,2025-07_r_users_per_text.csv,2025-08_r_users_per_text.csv,2025-09_r_users_per_text.csv"
