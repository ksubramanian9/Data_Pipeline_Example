# Assessment: Config-Driven Synthetic Generator Prototype

## Summary
- The prototype script already ingests the JSON schema, seeds the RNG, honours entity counts, and supports CSV or JSONL output with optional gzip compression, giving us an immediate baseline for schema-driven generation.
- It ships a library of field builders (UUIDs, sequenced IDs, ints, floats with multiple distributions, booleans, categories with weights or conditionals, strings with patterns, datetimes with ramp/seasonality, geographic coordinates, derived concatenations/maps) so the majority of fields in our proposed schema work without further coding.
- The generator keeps a `context` dictionary per record, letting derived fields depend on earlier values—this is essential for keys such as the concatenated composite identifier in the schema.
- Seasonal time windows are sampled with rejection weighting, which covers our stated need for hour-of-day/day-of-week profiles.

## How it compares to the legacy batch generator
- The pre-refactor batch tool (`services/batch/generate_synthetic_data.py` prior to this update) was purpose-built for retail orders with a fixed product catalogue, store roster, and CSV headers.
- The prototype replaces the bespoke loops with a declarative schema loader that can create any combination of fields, making it far easier to extend to new demos without rewriting Python logic.【F:docs/schema_generator_assessment.md†L5-L12】

## Gaps to close for full integration
1. **Field coverage review** – confirm the downstream Spark jobs and Kafka replay know how to interpret new column names/types. We may need a metadata file or CLI arguments to pass the schema path through the pipeline.
2. **Output directory semantics** – the existing batch pipeline expects one file per day, whereas the prototype writes a single flat file; we must decide whether to loop per day or adjust the ETL loader to accept a monolithic extract.
3. **Testing and packaging** – add unit tests (e.g., `pytest`) to exercise field generators and ensure deterministic behaviour under a seed; bundle the script as a module so services can import it rather than shelling out.
4. **Operational flags** – replicate convenience options from the current tool (auto-clearing the output directory, default start date calculation, etc.) if the batch pipeline relies on them.

## Recommendation
Adopt the script as the foundation for the refactor. Most heavy lifting for schema interpretation is already present, so remaining work centres on integrating with our pipeline assumptions and adding tests/documentation rather than implementing the core generation mechanics from scratch.
