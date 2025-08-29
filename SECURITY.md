# Security Policy

## Supported Versions

We take security seriously and actively maintain security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in KyroDB, please help us by reporting it responsibly.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities by emailing:
- **kishanvats2003@gmail.com**

### What to Include

When reporting a security vulnerability, please include:

1. **Description**: A clear description of the vulnerability
2. **Impact**: What an attacker could achieve by exploiting this vulnerability
3. **Steps to Reproduce**: Detailed steps to reproduce the issue
4. **Environment**: Your environment details (OS, version, etc.)
5. **Proof of Concept**: If possible, include a proof of concept

### Our Commitment

- We will acknowledge receipt of your report within 48 hours
- We will provide a more detailed response within 7 days indicating our next steps
- We will keep you informed about our progress throughout the process
- We will credit you (if desired) once the issue is resolved

### Security Best Practices

When using KyroDB in production, we recommend:

1. **Network Security**:
   - Run KyroDB behind a reverse proxy (nginx/caddy)
   - Use TLS/HTTPS in production
   - Restrict network access to necessary ports only

2. **Authentication**:
   - Enable authentication with strong tokens
   - Rotate tokens regularly
   - Use environment variables for sensitive configuration

3. **Access Control**:
   - Implement proper rate limiting
   - Monitor and log access patterns
   - Use firewalls and security groups

4. **Data Protection**:
   - Encrypt data at rest if required
   - Implement proper backup strategies
   - Monitor for unusual access patterns

5. **Monitoring**:
   - Enable comprehensive logging
   - Set up monitoring and alerting
   - Regularly review access logs

### Known Security Considerations

- **Default Configuration**: The default configuration prioritizes ease of use over security. Review and harden configurations for production use.
- **Memory Safety**: KyroDB is written in Rust, providing memory safety guarantees.
- **Data Persistence**: WAL and snapshot files contain sensitive data - protect these files appropriately.

### Security Updates

Security updates will be:
- Released as soon as possible
- Documented in release notes
- Announced through our communication channels
- Tagged with appropriate security advisories

### Contact

For security-related questions or concerns:
- Email: kishanvats2003@gmail.com
- Response Time: Within 48 hours

Thank you for helping keep KyroDB and its users secure!
