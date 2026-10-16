import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { catchError, throwError } from 'rxjs';

import { LogoutService } from './services/logout.service';

/**
 * Extract the access_token cookie value, if present.
 *
 * Note: this only checks the cookie EXISTENCE/format — actual JWT validity
 * is verified server-side. That's fine here because the interceptor uses
 * the cookie's presence as a signal that the user considers themselves
 * authenticated; the SERVER's 401 response is what tells us the cookie is
 * actually rejected.
 */
function readAccessToken(): string | null {
  const cookies = document.cookie.split(';');
  for (const cookie of cookies) {
    const parts = cookie.split('=');
    if (parts.length >= 2 && parts[0].trim() === 'access_token') {
      const raw = decodeURIComponent(parts.slice(1).join('='));
      return raw || null;
    }
  }
  return null;
}

/**
 * 401s from the login/MFA flow are part of normal validation — the user
 * typed the wrong OTP, captcha expired, etc. — and the login component
 * renders inline error messages for them. Only treat 401s from outside
 * that flow as 'your session is gone.'
 */
const AUTH_FLOW_PATH = /\/api\/auth\/(login|captcha|mfa\/(request|verify))\b/;

/**
 * Module-scoped single-flight flag: if multiple parallel requests all
 * 401 in the same tick (typical after token expiry), we want exactly one
 * logout + redirect, not N. LogoutService.logout itself is fast and
 * idempotent on persona/cookie, but Router.navigate is not free.
 */
let autoLogoutInFlight = false;

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const token = readAccessToken();
  const logout = inject(LogoutService);

  // Attach Authorization if we have a token. The backend's response —
  // including any 401 — then tells us whether the token was actually
  // accepted.
  const authedReq = token
    ? req.clone({ setHeaders: { Authorization: `Bearer ${token}` } })
    : req;

  return next(authedReq).pipe(
    catchError((err) => {
      if (
        err instanceof HttpErrorResponse &&
        err.status === 401 &&
        token &&                    // we had a token that the server rejected
        !AUTH_FLOW_PATH.test(req.url) &&
        !autoLogoutInFlight
      ) {
        autoLogoutInFlight = true;
        // LogoutService does everything: cookie clear, localStorage clear,
        // persona reset (signal + storage), navigate. Reusing it keeps the
        // auto-logout path identical to the manual-logout path in the
        // navbar — same code, same behaviour, no drift.
        logout.logout('/login');
        // Release the single-flight guard after the navigation kicks off.
        // A later unrelated 401 (e.g., after the user re-logs in and
        // that token also expires) should still trigger a fresh logout.
        setTimeout(() => (autoLogoutInFlight = false), 0);
      }
      return throwError(() => err);
    }),
  );
};
