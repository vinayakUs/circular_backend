"""Notification subsystem.

Public API (consumed by future MFA, password reset, suspicious-login
alerts, etc.):
    service = get_notification_service()
    result = service.send(NotificationRequest(...))
"""