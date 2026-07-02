# Security Policy

## Supported Versions

EchoFlow tracks a single rolling `main` branch. Security fixes are applied
there; there is no separate long-term-support branch at this time.

## Reporting a Vulnerability

If you find a security issue, please report it privately rather than
opening a public GitHub issue:

1. Open a [GitHub Security Advisory](https://github.com/Ashok007-cmd/echoflow-voice-pipeline/security/advisories/new)
   for this repository, or
2. Email the maintainer with details if advisories are unavailable to you.

Please include:
- A description of the issue and its potential impact
- Steps to reproduce (a minimal repro is ideal)
- Any relevant logs, stack traces, or PoC code

You should receive an acknowledgment within a few days. Once a fix is
available, a new release will be published and the reporter credited
(unless anonymity is requested).

## Scope

EchoFlow is a local CLI pipeline (no network listener or hosted API), so
most reports will fall into these categories:

- Dependency vulnerabilities (see `pip-audit` output in CI)
- Unsafe handling of API keys or credentials
- Injection risks in subprocess/audio-file handling
- Container hardening issues (`Dockerfile`, published images)

Reports about the third-party ASR/LLM/TTS provider APIs themselves
(OpenAI, Anthropic, Google, Microsoft Edge) should go to those vendors
directly.
