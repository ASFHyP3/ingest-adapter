# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

### Added
- Ingest `OPERA_RTC_S1_SLC` jobs if they do not already exist in CMR.

## [0.1.2]

### Fixed
- Added a 10s delay to the queue to wait for hyp3 to add the job to the database. Fixes [#33](https://github.com/ASFHyP3/ingest-adapter/issues/33).

## [0.1.1]

### Fixed
- Corrected typo in CloudFormation stack names.
- Resolved automation errors in the tag and release workflow.

## [0.1.0]

### Added
- Initial release.
