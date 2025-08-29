---
name: Bug Report
about: Report a bug or unexpected behavior
title: "[BUG] "
labels: ["bug", "triage"]
assignees: []

---

## Bug Description
A clear and concise description of what the bug is.

## Steps to Reproduce
1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

## Expected Behavior
A clear and concise description of what you expected to happen.

## Actual Behavior
What actually happened instead.

## Environment
- **OS**: [e.g., macOS 14.0, Ubuntu 22.04]
- **KyroDB Version**: [e.g., 0.1.0]
- **Rust Version**: [e.g., 1.70.0]
- **Go Version**: [e.g., 1.21.0]
- **Deployment**: [e.g., local, Docker, binary]

## Configuration
```yaml
# Relevant configuration (remove sensitive data)
KYRODB_PORT=3030
KYRODB_WARM_ON_START=1
# ... other relevant env vars
```

## Logs
```
# Paste relevant log output here
# Remove sensitive information
```

## Additional Context
- Is this a regression? (did it work in previous versions?)
- Any specific workload or data that triggers the issue?
- Performance impact?
- Workarounds attempted?

## Checklist
- [ ] I have searched existing issues for duplicates
- [ ] I have included all relevant environment information
- ] I have included steps to reproduce the issue
- [ ] I have included actual vs expected behavior
