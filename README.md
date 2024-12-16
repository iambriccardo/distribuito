# Distribuito

![Distribuito logo](./logo.png)

## Introduction

Distribuito is a column-oriented distributed database inspired by the Retriever database (https://www.youtube.com/watch?v=EMFKVimuyhQ) developed by Honeycomb. I decided to create Distribuito during Sentry's internal hackweek, a week-long hackathon where Sentry employees can work on any project they choose.

This project serves as a playground for me to explore database design and implementation.

Currently, the implementation is missing several features:
- The ability to filter by predicates.
- The ability to order results.
- The ability to express partitioning of data by columns.

## Features

- Column-oriented with nearly infinite scalability for adding new columns.
- File-based with an efficient custom file format (Apache Parquet support could be added in the future).
- Distributed query execution across multiple nodes.

_Please note that this project was created within 5 days during an internal hackathon at Sentry. The code quality, feature set, and stability are not ideal. It was mostly a fun experiment._
