# Architecture Decision Records (ADRs)

This directory contains Architecture Decision Records (ADRs) for the Agent-Orchestrated-ETL project.

## What is an ADR?

An Architecture Decision Record (ADR) is a document that captures an important architectural decision made along with its context and consequences.

## ADR Format

We use the format described by Michael Nygard in his article:

- **Title**: A short noun phrase describing the decision
- **Status**: Proposed, Accepted, Deprecated, or Superseded
- **Context**: The issue that motivates the decision
- **Decision**: The change that we're proposing or have agreed to implement
- **Consequences**: What becomes easier or more difficult to do because of this change

## ADR Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-001](001-agent-framework-selection.md) | Agent Framework Selection | Accepted |
| [ADR-002](002-workflow-engine-choice.md) | Workflow Engine Choice | Proposed |
| [ADR-003](003-configuration-management.md) | Configuration Management Strategy | Proposed |
| [ADR-004](004-monitoring-approach.md) | Monitoring and Observability Approach | Proposed |
| [ADR-005](005-security-architecture.md) | Security Architecture | Proposed |

## Creating a New ADR

1. Copy `template.md` to a new file with the next sequential number
2. Fill in the sections
3. Submit for review via pull request
4. Update this index when the ADR is accepted