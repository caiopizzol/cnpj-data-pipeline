# CHANGELOG

<!-- version list -->

## v1.11.1 (2026-04-01)

### Bug Fixes

- Use numbered files to prevent overwrite on flush
  ([`11c215e`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/11c215e5c97d750087fef031b3061659b49b2d02))


## v1.11.0 (2026-04-01)

### Features

- Log downloads at INFO when tqdm is disabled (Docker/CI)
  ([`54af1f3`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/54af1f372c9328cc518decc23e7c3c324975513a))


## v1.10.0 (2026-04-01)

### Features

- Add logging for parquet mode progress
  ([`bee3a4e`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/bee3a4eb2ac91125583bdd07ef357dc431534d91))

- Add per-file flush and POST_FILE_COMMAND hook for parquet mode
  ([`b26fa78`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/b26fa78ce53210d05290ea13492d93f91e8c24bb))

### Testing

- Add POST_FILE_COMMAND coverage
  ([`c674769`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/c674769f422594be5da6478549f0e89b938dc714))


## v1.9.0 (2026-04-01)

### Features

- Add per-file flush and POST_FILE_COMMAND hook for parquet mode
  ([#63](https://github.com/caiopizzol/cnpj-data-pipeline/pull/63),
  [`435f16a`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/435f16a54eb48c0f6ee982c19594e741fbe2beff))

- Per-file flush and POST_FILE_COMMAND for parquet mode
  ([#63](https://github.com/caiopizzol/cnpj-data-pipeline/pull/63),
  [`435f16a`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/435f16a54eb48c0f6ee982c19594e741fbe2beff))

### Testing

- Add POST_FILE_COMMAND coverage ([#63](https://github.com/caiopizzol/cnpj-data-pipeline/pull/63),
  [`435f16a`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/435f16a54eb48c0f6ee982c19594e741fbe2beff))


## v1.8.0 (2026-03-31)

### Bug Fixes

- Use date parsing instead of regex for calendar validation
  ([#61](https://github.com/caiopizzol/cnpj-data-pipeline/pull/61),
  [`356f728`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/356f72898aee7a6f4ada3e881450b76774e34539))

### Features

- Validar formato dos dados por tipo de campo
  ([#61](https://github.com/caiopizzol/cnpj-data-pipeline/pull/61),
  [`356f728`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/356f72898aee7a6f4ada3e881450b76774e34539))

- Validate field formats before loading
  ([#61](https://github.com/caiopizzol/cnpj-data-pipeline/pull/61),
  [`356f728`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/356f72898aee7a6f4ada3e881450b76774e34539))

### Refactoring

- Extract date_cols to shared _DATE_COLS constant
  ([#61](https://github.com/caiopizzol/cnpj-data-pipeline/pull/61),
  [`356f728`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/356f72898aee7a6f4ada3e881450b76774e34539))


## v1.7.0 (2026-03-31)

### Continuous Integration

- Add AI-powered release notes via semantic-release-ai-notes action
  ([`8764d72`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/8764d729df389337151fbb35f1d805b4483e1770))

### Documentation

- Add all three CNPJ dataset resources to README
  ([#58](https://github.com/caiopizzol/cnpj-data-pipeline/pull/58),
  [`6fba647`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/6fba647595f93945593e3906e3bcddf441babfbf))

- Documentar origem legal e oficial dos dados
  ([#58](https://github.com/caiopizzol/cnpj-data-pipeline/pull/58),
  [`6fba647`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/6fba647595f93945593e3906e3bcddf441babfbf))

- Fix links to use official gov.br URLs for Notas Tecnicas and metadados
  ([#58](https://github.com/caiopizzol/cnpj-data-pipeline/pull/58),
  [`6fba647`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/6fba647595f93945593e3906e3bcddf441babfbf))

### Features

- Add Parquet output format ([#62](https://github.com/caiopizzol/cnpj-data-pipeline/pull/62),
  [`5a1b47f`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/5a1b47f82c70fec80c16698a756555655e97bbce))

### Testing

- Add parquet output tests for main.py
  ([#62](https://github.com/caiopizzol/cnpj-data-pipeline/pull/62),
  [`5a1b47f`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/5a1b47f82c70fec80c16698a756555655e97bbce))

- Add parquet_writer tests ([#62](https://github.com/caiopizzol/cnpj-data-pipeline/pull/62),
  [`5a1b47f`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/5a1b47f82c70fec80c16698a756555655e97bbce))


## v1.6.0 (2026-03-31)

### Documentation

- Document loading strategy in README
  ([#57](https://github.com/caiopizzol/cnpj-data-pipeline/pull/57),
  [`8908dfd`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/8908dfd64e020c4cf207fbae593b99d268d4eba7))

### Features

- Add TRUNCATE+INSERT loading strategy as alternative to UPSERT
  ([#57](https://github.com/caiopizzol/cnpj-data-pipeline/pull/57),
  [`8908dfd`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/8908dfd64e020c4cf207fbae593b99d268d4eba7))

- Adicionar estratégia TRUNCATE+INSERT para carga completa
  ([#57](https://github.com/caiopizzol/cnpj-data-pipeline/pull/57),
  [`8908dfd`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/8908dfd64e020c4cf207fbae593b99d268d4eba7))


## v1.5.0 (2026-03-31)

### Continuous Integration

- Run CI on push to main and release only after CI passes
  ([`34d590b`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/34d590b096aaf660da457072c0b4b4809250510a))

### Features

- Validate data quality before loading
  ([`9a0bcb8`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/9a0bcb80d4f178ea1a6bc1788f9dafa4b82d3ba9))


## v1.4.0 (2026-03-31)

### Features

- Use Polars read_csv_batched and add integration tests
  ([`93a8560`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/93a8560cda91fc451325d50215df80bc65069880))

### Testing

- Add tests for database, main, and processor gaps
  ([`c461630`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/c461630ab25be581b8f2ce63bc3fc29d905794dd))


## v1.3.3 (2026-03-31)

### Bug Fixes

- Prevent silent data loss and improve reliability
  ([`44f3b24`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/44f3b240396be795d0e2429d29394b764561898c))

### Chores

- Add issue templates for bug and feature
  ([`0cbb27f`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/0cbb27f8e3e637f4d0d27eae068aebd275e753ec))

- Add lefthook pre-commit
  ([`010582e`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/010582eca1f72eae9358c86adf72be4f97190ef7))

- Update readme
  ([`fdf4704`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/fdf4704a0a663b9e7b5d9a847f59ddb5ed1dfe13))

### Documentation

- Add contributors section and auto-update workflow
  ([`cd05529`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/cd05529265251276e90467f0795182762dcbd036))

- Italic
  ([`9a6efc4`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/9a6efc4f19d4eed06e216d5967f2229c3de1145d))

- Update nextcloud url
  ([`75327d6`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/75327d6efcdf1971bfbb55050e362bc7d8b97ff7))

- Update readme with centered layout and cnpj.chat link
  ([`f352d73`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/f352d73e404f6438c6c6a4b1f0aa75c3afb9cb1e))


## v1.3.2 (2026-02-09)

### Bug Fixes

- Adjust downloader to new url
  ([`b1cd64a`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/b1cd64a233d7d02109451d4017f361a445182b1f))

### Chores

- Add badges
  ([`60b3c63`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/60b3c63376dd33b8bdc9edbc2437a18f1bda5941))

- Add codecov
  ([`3a050df`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/3a050df5b86c3edc520b05a862b2f7be05a135bf))

- Fix badge repo
  ([`0f91ae7`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/0f91ae773381823cf59c48867771fb96cdf158a9))

- Long lived container
  ([`29dfeb8`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/29dfeb816124f429ee491d9bfdfc30de6bd2f5db))

- Pre-commit and ci for lint, format and test
  ([`0bfcb0c`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/0bfcb0c003fc5a92988f1e317fb7693220bf364c))

### Documentation

- Fix tables relationship
  ([`4e4c062`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/4e4c062d873a8f9e2f2560151e5bf4306d747e63))

- Update readme + new data-schema docs
  ([`02f4cd8`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/02f4cd8c0748ac7249d342648b35ffb241a3d011))

### Testing

- Add tests for processor and downloader modules
  ([`6ad6d65`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/6ad6d650427da578b6a08a55629ce149e817162b))


## v1.3.1 (2026-01-07)

### Bug Fixes

- Resolve Simples.zip processing issue
  ([`27d8fb3`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/27d8fb3ea2256670e5d8b2e86964fbb454adfbf8))

### Refactoring

- Clean up test fixtures and remove redundant tests
  ([`fa8a236`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/fa8a236f36f2afa3491d86520d69628128981ca6))

### Testing

- Add regression tests for Simples.zip processing
  ([`dca665e`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/dca665e0e9b7aa15a4ea75f99e38b7bd2534d00e))


## v1.3.0 (2025-12-15)

### Features

- Polars read from csv utf-8
  ([`4376bdc`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/4376bdc2817530513d1fc35f7f6813816b824b78))


## v1.2.0 (2025-12-15)

### Features

- Process older months
  ([`6dd3f6d`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/6dd3f6d19a6e74f72bf771d91272bc7ec8caee90))


## v1.1.0 (2025-12-15)

### Bug Fixes

- Changelog release
  ([`cf68825`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/cf68825c2b1da03a8f2880d48ba758d1cbd23543))

- Remove changelog
  ([`9f65c59`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/9f65c59bc929b089672f1a2b29cc08a5c5ec3411))

### Features

- Keep downloaded files + skip on localdev
  ([`a551a0b`](https://github.com/caiopizzol/cnpj-data-pipeline/commit/a551a0b821db997a4d56aebcc167145b7e6e7cb6))


## v1.0.0 (2025-12-15)

- Initial Release
