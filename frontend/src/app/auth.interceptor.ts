import {
  HttpErrorResponse,
  HttpInterceptorFn,
  HttpRequest,
} from '@angular/common/http';
import { inject } from '@angular/core';
import { catchError, switchMap, throwError } from 'rxjs';

import { AuthService } from './services/auth.service';
import { LogoutService } from './services/logout.service';

/** Methods that require X-CSRF-Token. */
const UNSAFE = /^(POST|PUT|PATCH|DELETE)$/i;

/**
 * Paths whose 401s are normal validation rather than a dead session: a
 * mistyped OTP, an expired captcha, bad credentials. Logging the user out
 * on those would throw away a good session because of a typo.
 *
 * /api/auth/session must be included or a genuine logout retries forever,
 * since the refresh call itself 401s when there is no session.
 */
const AUTH_FLOW_PATH =
  /\/api\/auth\/(login|captcha|mfa\/(request|verify)|session)\b/;

/**
 * Same-origin today, but withCredentials keeps this correct if the API is
 * ever split onto another origin.
 */
function withAuth(
  req: HttpRequest<unknown>,
  csrf: string | null,
): HttpRequest<unknown> {
  let out = req.clone({ withCredentials: true });
  if (UNSAFE.test(req.method) && csrf) {
    out = out.clone({ setHeaders: { 'X-CSRF-Token': csrf } });
  }
  return out;
}

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const auth = inject(AuthService);
  const logout = inject(LogoutService);

  return next(withAuth(req, auth.csrf())).pipe(
    catchError((err) => {
      // A 401 while holding a session almost always means the access token
      // just expired, so rotate it and replay rather than logging out.
      // 'unknown' and 'anonymous' are excluded — nothing to refresh with.
      const recoverable =
        err instanceof HttpErrorResponse &&
        err.status === 401 &&
        !AUTH_FLOW_PATH.test(req.url) &&
        auth.isAuthenticated();

      if (!recoverable) return throwError(() => err);

      return auth.refreshOnce().pipe(
        switchMap((session) => {
          if (!session) {
            logout.logout('/login');
            return throwError(() => err);
          }

          // Rebuilt from the ORIGINAL req, not the outgoing one: the refresh
          // rotated the csrf_token, and replaying the stale one would 403
          // every unsafe request right after a successful refresh.
          return next(withAuth(req, auth.csrf()));
        }),
      );
    }),
  );
};
