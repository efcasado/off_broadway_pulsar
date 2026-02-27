## [1.3.0](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.11...v1.3.0) (2026-02-27)

### Features

* add support for multi-topic producers ([#39](https://github.com/efcasado/off_broadway_pulsar/issues/39)) ([7e37e29](https://github.com/efcasado/off_broadway_pulsar/commit/7e37e29ac2a46229b3f7697ce9e851a9f4499924))

## [1.2.11](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.10...v1.2.11) (2026-02-27)

## [1.2.10](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.9...v1.2.10) (2026-02-20)

## [1.2.9](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.8...v1.2.9) (2026-02-04)

## [1.2.8](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.7...v1.2.8) (2026-02-03)

## [1.2.7](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.6...v1.2.7) (2026-01-29)

## [1.2.6](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.5...v1.2.6) (2026-01-22)

## [1.2.5](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.4...v1.2.5) (2026-01-22)

## [1.2.4](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.3...v1.2.4) (2026-01-09)

## [1.2.3](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.2...v1.2.3) (2025-12-27)

## [1.2.2](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.1...v1.2.2) (2025-12-18)

## [1.2.1](https://github.com/efcasado/off_broadway_pulsar/compare/v1.2.0...v1.2.1) (2025-12-15)

### Bug Fixes

* missing chunking-related options ([296d462](https://github.com/efcasado/off_broadway_pulsar/commit/296d46210901f3ca6d4acb8095fb21c9184d968c))
* missing compaction-related settings ([8868e05](https://github.com/efcasado/off_broadway_pulsar/commit/8868e053f759af62ce127cddf6251dd7e173ea12))

## [1.2.0](https://github.com/efcasado/off_broadway_pulsar/compare/v1.1.0...v1.2.0) (2025-12-14)

### Features

* add support for message chunking ([29facd1](https://github.com/efcasado/off_broadway_pulsar/commit/29facd1433884223cc0f481c6cb7cfabbe0b9f87))

## [1.1.0](https://github.com/efcasado/off_broadway_pulsar/compare/v1.0.4...v1.1.0) (2025-12-06)

### Features

* support pulsar-elixir multi-client architecture ([93ba3fc](https://github.com/efcasado/off_broadway_pulsar/commit/93ba3fc87b97c6b89d5197764485d744e71ff5a5))

## [1.0.4](https://github.com/efcasado/off_broadway_pulsar/compare/v1.0.3...v1.0.4) (2025-12-05)

## [1.0.3](https://github.com/efcasado/off_broadway_pulsar/compare/v1.0.2...v1.0.3) (2025-12-05)

## [1.0.2](https://github.com/efcasado/off_broadway_pulsar/compare/v1.0.1...v1.0.2) (2025-12-05)

## [1.0.1](https://github.com/efcasado/off_broadway_pulsar/compare/v1.0.0...v1.0.1) (2025-12-05)

## 1.0.0 (2025-12-05)

### Features

* optional pulsar connection ([0ebb96b](https://github.com/efcasado/off_broadway_pulsar/commit/0ebb96b6776b031a0992e9ebd656b06d224e3e87))
* producer concurrency ([d430692](https://github.com/efcasado/off_broadway_pulsar/commit/d430692a76729618ed41df881d2eb875a1f7203c))
* support all pulsar-elixir options ([454d20e](https://github.com/efcasado/off_broadway_pulsar/commit/454d20e5d75bd409ae3c8da8eb46837ae38bff7f))

### Bug Fixes

* align demand handling with pulsar native flow control ([f9ba1de](https://github.com/efcasado/off_broadway_pulsar/commit/f9ba1defb2306a02cfb6aa7742198bbc6ffcd27e))
* back pressure bug ([40faa92](https://github.com/efcasado/off_broadway_pulsar/commit/40faa9257a3e3c050f2a47585ef6165b34c42454))
* batched acknowledgements ([a2a094e](https://github.com/efcasado/off_broadway_pulsar/commit/a2a094e880092d130f7057278fcb99320a036bb2))
* dispatch message when pipeline waiting for demand ([0484272](https://github.com/efcasado/off_broadway_pulsar/commit/0484272a0c79059bbe351a74d50321ec09526cf3))
* dispatch messages in order ([d805b98](https://github.com/efcasado/off_broadway_pulsar/commit/d805b9827248b1aa301dac759b1f4e6d746fa922))
* do not ignore startup delay and jitter ([10097ec](https://github.com/efcasado/off_broadway_pulsar/commit/10097ec31ee4828ca61185d2260c33cbbdcddcaf))
* do not start pulsar app automatically ([ffb8c22](https://github.com/efcasado/off_broadway_pulsar/commit/ffb8c222c9dc1cbba3178cb5cf12013e53555c7a))
* dynamic pulsar consumer process ([a632b1e](https://github.com/efcasado/off_broadway_pulsar/commit/a632b1e7f35ee61ee4a577cb4c25ac8b009ddbcb))
* only dispatch messages based on demand ([81d34e9](https://github.com/efcasado/off_broadway_pulsar/commit/81d34e92f1f165783319df13ae719be4e0525dfa))
* pipeline stops on consumer restart ([42e0324](https://github.com/efcasado/off_broadway_pulsar/commit/42e03244280dd86c4c5f4e17cc2f43e96989c374))
* stale consumer when using DLQ policy ([da89a9d](https://github.com/efcasado/off_broadway_pulsar/commit/da89a9da31ae0b0afc5c5f4dd42a8fafd3641fe4))
* wrong function clause in consumer ([f2e1625](https://github.com/efcasado/off_broadway_pulsar/commit/f2e1625fba1ebb234a02d70e97a0f719adc5cad5))
