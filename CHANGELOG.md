# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **PODAAC-5589**
  - enable granule_id/identifier/product_name to be manually set, fixing a .tar.gz issue
### Changed
### Deprecated
### Removed
### Fixed
- **PODAAC-5947**
  - Update to use Python 3.9 and cumulus-process 1.3.0 
### Security
- **Dependabot**
  - Update `pyproject.toml` to use latest `moto 4.1.14` thus fixing certifi and cryptography versions

## [v0.2.0] - 2022-08-02

### Added
- **PODAAC-4119**
  - Changed `PROVIDER` field to be dynamically reading the ID instead of hardcoding to `PODAAC`
### Deprecated
### Removed
### Fixed
- Issue with `actions-ecosystem/action-push-tag@v1` during github action build; reverting to manual process: https://github.com/actions-ecosystem/action-push-tag/issues/10
### Security

## [v0.1.0] - 2022-03-17

### Added
- **PODAAC-4122**
  - Working function and test, along with this document and README.rst document
  - Build script
### Deprecated
### Removed
### Fixed
### Security