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
   - Use TLS/HTTPS in production with `--tls-cert` and `--tls-key`
   - Restrict network access to necessary ports only
   - Use firewall rules to limit access to trusted networks

2. **Authentication & Authorization**:
   - Enable authentication with strong tokens using `--auth-token` and `--admin-token`
   - Use separate tokens for read/write vs admin operations
   - Rotate tokens regularly using secure random generation
   - Use environment variables for sensitive configuration
   - Implement role-based access control (Admin/ReadWrite/ReadOnly)

3. **Access Control**:
   - Implement proper rate limiting (configurable via env vars)
   - Monitor and log access patterns
   - Use firewalls and security groups
   - Enable comprehensive HTTP logging with `KYRODB_DISABLE_HTTP_LOG=1` to disable if needed

4. **Data Protection**:
   - Encrypt data at rest if required
   - Implement proper backup strategies
   - Monitor for unusual access patterns
   - Protect WAL and snapshot files with appropriate permissions

5. **Monitoring**:
   - Enable comprehensive logging
   - Set up monitoring and alerting on `/metrics` endpoint
   - Regularly review access logs and rate limiting events
   - Monitor `/build_info` for provenance verification

### TLS Configuration

KyroDB supports TLS/HTTPS for encrypted connections:

```bash
# Production TLS setup
./kyrodb-engine serve 0.0.0.0 3030 \
  --tls-cert /path/to/certificate.pem \
  --tls-key /path/to/private-key.pem
```

- Supports modern TLS 1.3 and 1.2
- Compatible with Let's Encrypt certificates
- Automatic ALPN protocol negotiation (HTTP/2, HTTP/1.1)

### Role-Based Access Control (RBAC)

KyroDB implements three permission levels:

- **Admin**: Full access to all operations including system management
  - Snapshot creation (`POST /v1/snapshot`)
  - Compaction (`POST /v1/compact`)
  - RMI rebuild (`POST /v1/rmi/build`)
  - System warmup (`POST /v1/warmup`)
  - All data operations

- **Read/Write**: Standard data operations
  - Data insertion and updates
  - SQL queries and modifications
  - Vector operations
  - Read operations

- **Read-Only**: Read-only access
  - Data lookups and queries
  - Event replay and subscription
  - System status endpoints

### API Security Headers

KyroDB includes security headers for API responses:
- `Content-Type` validation
- Proper HTTP status codes for authentication/authorization failures
- Rate limiting with appropriate status codes

### Known Security Considerations

- **Default Configuration**: The default configuration prioritizes ease of use over security. Review and harden configurations for production use.
- **Memory Safety**: KyroDB is written in Rust, providing memory safety guarantees.
- **Data Persistence**: WAL and snapshot files contain sensitive data - protect these files appropriately.
- **Token Storage**: Bearer tokens should be stored securely and rotated regularly.
- **Network Encryption**: Always use TLS in production environments.
- **Rate Limiting**: Configure appropriate rate limits to prevent abuse.

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
