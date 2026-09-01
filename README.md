# PySpark learning journey

Daily notes and example code as I learn PySpark, from zero to job-ready for a data analyst role.

## How this repo works

- Each folder is one day: `day-01-topic-name`, `day-02-topic-name`, etc.
- Every day folder has two files:
  - `notes.md` — what I learned, in my own words
  - `example.py` — runnable code demonstrating it
- `template/` has a blank day folder to copy for the next entry.

## Log

| Day | Topic | Notes |
|-----|-------|-------|
| 01  | Intro to PySpark, cluster architecture, lazy evaluation, first DataFrame | [notes](day-01-intro-and-architecture/notes.md) |
| 02  | Joins (inner/left/right/full) and handling nulls | [notes](day-02-joins-and-nulls/notes.md) |
| 03  | Window functions — rank, dense_rank, row_number, running totals | [notes](day-03-window-functions/notes.md) |

## Setup (to run examples locally)

```bash
pip install pyspark
python day-01-intro-and-architecture/example.py
```
