# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-alpha.1] - 2024-11-20

### Added
- Initial alpha release of snowforge-py
- Core Features:
  - Forge class for Snowflake operations with connection management
  - Builder pattern implementations for all major components
  - Transaction and session management support
- Data Definition Language (DDL) Operations:
  - Table creation and management
  - Stage operations (internal and external)
  - File format handling (CSV, JSON, Avro, Parquet, ORC, XML)
  - Stream creation and configuration
- Data Loading:
  - PUT operations with compression support
  - COPY INTO operations with extensive options
  - Pattern matching and file format specifications
- Task Management:
  - Task creation and scheduling
  - Support for different warehouse sizes
  - Cron and interval scheduling
- Workflow Orchestration:
  - Database and schema management
  - Tag management and application
  - Multi-step workflow support
- Documentation:
  - Comprehensive examples for all major features
  - Type hints throughout the codebase
  - Working examples

### Dependencies
- Python >=3.9,<3.12
- snowflake-snowpark-python ^1.24.0