# Security Policy

## Supported Versions

We actively maintain security patches for the latest stable releases of Milvus. Please ensure you are using an updated version to avoid known vulnerabilities.

---

## Reporting a Vulnerability

If you discover a security issue, please report it responsibly by opening a private issue or contacting our security team directly at **security@milvus.io**. Do **not** create a public issue for security vulnerabilities.

---

## Known Vulnerabilities

The following critical vulnerabilities have been identified in Milvus:

### 1. Unauthenticated Access to RESTful API on Metrics Port (9091)

- **GHSA ID:** GHSA-7ppg-37fh-vcr6  
- **Published:** Feb 11  
- **Severity:** Critical  
- **Description:** Unauthenticated access to the metrics port may allow attackers to gather sensitive system metrics and compromise critical components.  
- **Impact:** Critical system compromise.  
- **Remediation:** Ensure metrics port (9091) is not publicly accessible and enforce authentication. Upgrade to the latest patched version.

### 2. Authentication Bypass Vulnerability in Milvus Proxy

- **GHSA ID:** GHSA-mhjq-8c7m-3f7p  
- **Published:** Nov 10, 2025  
- **Severity:** Critical  
- **Description:** Critical authentication bypass in Milvus Proxy can allow unauthorized users to access and manipulate data.  
- **Impact:** Full system compromise and potential data leakage.  
- **Remediation:** Upgrade to the latest version of Milvus Proxy where this vulnerability is patched.

---

## Staying Updated

For more information on security advisories related to [milvus-io/milvus](https://github.com/milvus-io/milvus/security/advisories) visit the GitHub Advisory Database. Regularly check for updates and apply security patches promptly.

---

## Security Best Practices

1. Always restrict access to internal ports such as 9091.  
2. Enable authentication and role-based access control in Milvus.  
3. Monitor your system for unusual activity.  
4. Keep your Milvus deployment up-to-date with the latest security patches.  
5. Use firewalls and network segmentation to limit exposure.  

---

> **Note:** This security policy is intended to help contributors and users understand vulnerabilities and how to mitigate risks in Milvus deployments.
