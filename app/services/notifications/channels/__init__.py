"""Channel strategies: NotificationChannel ABC + concrete transports.

Each channel owns its own transport code (smtplib, requests, etc.) —
no separate transport/ sub-package. The channel is the unit that knows
both the delivery semantics (email vs SMS) and the protocol details
needed to talk to the wire.
"""