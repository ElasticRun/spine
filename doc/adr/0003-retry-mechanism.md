# 3. Retry Mechanism

Date: 2019-06-20

## Status

Accepted

## Context

Message processing by applications can fail, for various reasons. Some of the failures might be transient while other permanent. Hence, Spine Adapter component that allows the applications to use Kafka as integration bus, should incorporate mechanism to retry message processing, in case of failures. 

## Decision

Spine Adapter will use reasonable defualts for retrying failed messages. It will also provide a hook for the applications to decide the correct retry mechanism for each of the messages being processed. Applications can imlement the logic as part of message handler and respond with retry schedule for the message, as appropriate. If no retry schedule is provided, default schedule will be used. If retry mechanism is not valid for a given message, the handler should respond with a blank/empty retry mechanism for such cases.

## Consequences

- Every message published to kafka can be processed multiple times, until it is successfully processed or all retries fail. This introduces an additional status for messages to be in. Along with Error and Success statuses for messages, they can also be in "Retry Pending" status, with a scheduled time for next retry.
- History of retries and their corresponding status needs to be maintained for each log to ensure messages give up after the defined number of retries.
- Consumers of the common spine adapter, also need to include logic to decide on retry mechanism for each message, to take advantage of the new feature.
