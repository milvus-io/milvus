# Norms of Milvus Log Output

## Concensuses

- By default, Milvus only prints `Info`-level logs.
- ZAP Logger supports logs of `Info`, `Debug`, `Warn`, and `Error` levels.
- OpenTracing supports `Trace`-level logs.

## Rules

- Designate proper level for logs.
- Use explicit words to indicate key information and avoid abbreviations in logs. `Info`-level logs should be readily comprehensible to the non-developers, and logs of other levels should be comprehensible to all developers.
- By the principle of High Cohesion, avoid defining the same log in different places.
- While defining key `struct`, implement the `ToString` function to facilitate the log output.
- Avoid flooding the screen with logs.

## Log level

Log levels are listed from low to high as follows:
- `Trace`: Developer-oriented, mainly carries the detailed information for performance monitoring and adjustment.
- `Debug`: Developer-oriented, mainly carries the specific parameters for bug tracing and adjustment, for example, configuration data passed to the system, serializing or de-serializing actions while underlying communication protocols publish or receive requests. Debug-level logs should contain as many details as possible. However, temporary logs made for the convenience of development must be eliminated.
- `Info`: User-and-developer-oriented, mainly carries the information of critical procedures, for example, configuration service start & stop, successful registry, and update in metadata that has been monitored. Info-level logs must be printed at the critical paths at a low frequency.
- `Warn`: User-and-developer-oriented, mainly carries the information of exceptions that are expected to occur and can be handled properly. Warn-level logs should contain as many details as possible.
- `Error`: User-and-developer-oriented, mainly carries the information of exceptions that cannot be handled properly and will affect the system operation, for example, unsuccessful registry and abnormal interruption of the service.
- `Panic`: User-and-developer-oriented, mainly carries the information of recoverable errors.
- `Fatal`: User-and-developer-oriented, mainly carries the information of unrecoverable errors.


## Log format

- Avoid abbreviations of keywords in logs. For example, do not abbreviate `timetick` as `tt`.
- Terms that are conventionally abbreviated must be in upper case. For example, use `ID` instead of `id`.
- Use lower case when defining error message strings. For example, use `Error("wrong result")` instead of `Error("Wrong result")`.
- Do not use the combination of multiple strings in logs. For example, use `log.Debug("something failed", zap.Int64("ID", ID), zap.Error(err))` instead of `log.Debug(fmt.Sprintf("something failed with ID: %d", ID) + " with error: " + err.Error())`, and use `fmt.Errorf("DML flow graph doesn't existed, collectionID = %d", collectionID)` instead of `errors.New("DML flow graph doesn't existed, collectionID = " + fmt.Println(collectionID))`.
- Arrange keywords in subordination order. For example, when a log contains component name, collection ID, and RPC ID, the order should be RPC ID, component name, and then collection ID.


## Best Practice
- Must do:
  - Print `Info`-level logs when the system or service initiates or stops.
  - Print `Error`-level logs with more details when an error is returned or `Panic`-level log is triggered.
- Can do:
  - Print `Debug`-level logs during RPCs.
  - Print `Debug`-level logs when calling, reading, or writing slow logs.
  - Print `Debug`-level logs during periodic background tasks or when monitoring background tasks.
- Log frequency:
  - Be prudent with the frequency of log output, and avoid flooding the screen with log.
  - Generally, only `Debug`-level logs are allowed to be printed at a high frequency.
