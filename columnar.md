# Columnar file formats

Static columnar file format is being highlighted since Dremel had been
disclosed from Google. Dremel (or at least technology in it) is in the
core of Google's data platform like BigQuery - consequently there are
several columnar formats with open implementation emerging in the
wild. This document is a survey on these formats; for the first time
the targets are *Parquet*, *ORCFile* and *Supersonic* .

The format matters, because the format itself is very important factor
of MPP systems recently emerging - such as Impala, Presto and
Hive. They all uses columnar format to reduce the cost of
full-scanning the table. And each format differs in its performance
tendency - I believe one does not fit all use cases.

## Summary

- ORCFile
- Parquet
- Supersonic


# ORCFile

ORCFile stems from *RCFile* format, which is originally used as one of
optimized file formats of Hive.

It has 250MB slot and consists of header/column index, and footer.

It uses protobuf.

# Parquet

Parquet came from Twitter

It uses thrift by default - but the format is configurable.


# Supersonic

Supersonice came from Google.

Currently it's all on memory.
