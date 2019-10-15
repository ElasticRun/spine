# 2. Use Apache Kafka as Integration Backbone

Date: 2019-04-30

## Status

Accepted

## Context

Multiple different platforms currently integrate with each other in custom way, leading to multiple different integration mechanisms and approaches. It is required to standardize the way integration is implemented, while ensuring loose coupling between different systems.

## Decision

Use Apache Kafka based integration backbone that will provide the required infrastructure to implement event based integrations instead of pull/push type of integrations.

## Consequences

- All Frappe based platforms can use a standard reusable application for publishing and consuming the events
- All producer platforms will only be required to decide on the events to be published and not how these should be consumed or processed.
- Individual consumer platforms will only be required to decide on the action to be taken when a particular event is received, ideally, irrespective of the source of the event.
