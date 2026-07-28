# Persisted data contract invariant

Each RFB record has one compatible contract across positional CSV parsing, transformations, PostgreSQL loading, Parquet output, recipes, and documentation. A source layout, output column, type, table name, or identity change must keep `COLUMNS`, `OUTPUT_COLUMNS`, transformation output order, `initial.sql`, conflict behavior, Parquet schema versioning, recipe joins and projections, migration guidance, and contract tests aligned wherever they are affected. PostgreSQL and Parquet may use different physical types or metadata columns, but they must preserve the same logical source rows and stable identities.

Identity keys must use the stable canonical fields that distinguish real records. Masked or non-unique source attributes cannot be assumed unique, and mutable attributes cannot become identity fields merely to resolve a collision. Derived recipes may intentionally expose a narrower or enriched schema when their documented contract says so; they must still join through the current canonical keys without multiplying or dropping source rows unintentionally.

Violating this invariant can silently assign values to the wrong fields, collapse distinct records during upsert, break joins, make loading strategies disagree, or publish a Parquet schema whose metadata does not warn consumers about an incompatible change.
