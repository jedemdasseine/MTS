#!/usr/bin/env bash
sqlite3 MTSDB.db ""
python3 generate.py --count 100000 --output MTSDB.db
python3 calc_stats.py --db MTSDB.db