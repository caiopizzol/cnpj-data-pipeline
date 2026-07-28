# Pipeline completion invariant

A source file or source month is complete only after every selected output is durable. PostgreSQL processing must commit every batch and record the source with `mark_processed(directory, filename)` before deleting the extracted CSV. Parquet processing must close each table successfully, complete any configured post-file command, and close all tables before publishing the success manifest. Worker, transformation, write, commit, flush, and post-file-command failures must reach the coordinator and make the run fail.

Expected exceptions are inputs explicitly skipped because the same source directory and filename were already recorded as processed, temporary encoding copies removed while the original source remains recoverable, and cleanup performed after the durable output or an explicit failure is already known. `KEEP_DOWNLOADED_FILES` changes retention only; it does not change completion criteria.

Violating this invariant can publish an incomplete month, permanently skip missing rows on the next run, delete the only recoverable input, or present stale output metadata as a successful export.
