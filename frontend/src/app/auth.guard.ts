import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { catchError, map, of } from 'rxjs';

import { AuthService } from './services/auth.service';

/**
 * Both guards read AuthService synchronously when the status is already
 * known and only hit the network when it is 'unknown'; bootstrap resolves
 * the session before first render, so the synchronous branch is the common
 * case and navigation costs no HTTP.
 *
 * On a network error we ALLOW navigation rather than redirecting: a dropped
 * connection is not a logout. Nothing protected is exposed — every API call
 * still requires the cookie, so the shell renders and its data fails to load.
 */
export const authGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (auth.status() !== 'unknown') {
    return auth.isAuthenticated() ? true : router.createUrlTree(['/login']);
  }

  return auth.session().pipe(
    map((session) =>
      session ? true : router.createUrlTree(['/login']),
    ),
    catchError(() => of(true)),
  );
};

/** Guest-only routes (/login): a signed-in user is sent home. */
export const guestOnlyGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (auth.status() !== 'unknown') {
    return auth.isAuthenticated() ? router.createUrlTree(['/']) : true;
  }

  return auth.session().pipe(
    map((session) => (session ? router.createUrlTree(['/']) : true)),
    catchError(() => of(true)),
  );
};
