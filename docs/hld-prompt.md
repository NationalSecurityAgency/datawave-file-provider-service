# Context Service High-Level Design

## Instructions
- Produce a high-level design document that we can use to further expand the architecture.
- Use a YAGNI principle to keep the design initially simple, we can iterate on complexity later where needed.
- Use Sonnet agents to research or deep-dive code repositories.
- Launch an advesarial agent to evaluate the high-level design when complete which reviews against guidance/instructions.
  - The agent should also evaluate for simplicity, seeing if any architectural component can be further simplified.
- Review/research [1] [2] (they may be added as directory for the session).

## Overview

Context service provides context management for Accumulo ClassLoader (HDFS) representations [1]. 

The following is provided by the service:
- Context component management and awareness.
- Auto-detection of new files that need to be added to the context.
- Context pointer management (i.e. current context).

## Guidance / Advice
- Evaluate Datawave microservice starters, this will be a Datawave microservice [2].
- Manifest of configured files will be used as a declarative intruction for configured files that we are serving via context.
  - This does not replace the Accumulo class loader context; which is the source of truth of the actual files.
- The class loader requires a JAR, the files will need to be encoded into a JAR for serving.
- Use Java as a programming platform.
- Pointer management should be SPI driven.
  - Consider implementing a Zookeeper pointer management implementation by default.
- Evaluate how callers should upload new files to the context, and how the context detects when the uploads are finished.
  - Consider if callers need to provide upload advice to an API or if the service will auto-detect new files.
  - Consider how the service would supersede other files, perhaps a date convention is used.
- Develop an audit trail that someone can observe.
  - Evaluate if the audit trail should live in a git repo (it should be SPI driven).
  - Avoid a database, try to keep simple, using a component we already have available.
  - Exposed the audit trail via API.


[1]: https://github.com/apache/accumulo-classloaders
[2]: https://github.com/NationalSecurityAgency/datawave-spring-boot-starter
