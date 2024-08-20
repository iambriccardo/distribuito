# Distribuito

Distribuito is a column-oriented distributed database inspired by the Retriever database 
(https://www.youtube.com/watch?v=EMFKVimuyhQ) developed by Honeycomb. I decided to create Distribuito during Sentry's 
internal hackweek, a week-long hackathon where Sentry employees can work on whatever they want.

This project is intended to be a playground for me to learn about database design and implementation. Currently, the 
implementation lacks many features that I might add in the future.

## Features
- Column-oriented with nearly infinite scalability for adding new columns.
- File-based with an efficient custom file format (Apache Parquet support could be added in the future).
- Distributed query execution across multiple nodes (still TBD).