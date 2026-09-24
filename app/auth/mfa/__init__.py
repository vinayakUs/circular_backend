"""MFA subsystem.

Manages OTP challenge lifecycle and pending-token lifecycle for the
multi-factor authentication flow:

  /api/auth/login       → mints mfa_pending_token (captcha+password+DB ok)
  /api/auth/mfa/request → sends OTP via NotificationService
  /api/auth/mfa/verify  → verifies OTP, mints JWT

Mirrors the captcha module's pattern: ABC + Postgres impl + composition
root in make_services.py.
"""
