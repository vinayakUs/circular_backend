import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { LoginService } from './services/login.service';

/**
 * Route guard for protected routes.
 *
 * Allows access only if the user is authenticated.
 * Otherwise redirects to /login.
 */
export const authGuard: CanActivateFn = (_route, _state) => {
  const auth = inject(LoginService);
  return auth.isAuthenticated() || inject(Router).createUrlTree(['/login']);
};

/**
 * Route guard for guest-only routes (e.g., /login, /signup).
 *
 * Allows access only if the user is NOT authenticated.
 * Otherwise redirects to / (home) so a logged-in user never sees
 * the login form again.
 */
export const guestOnlyGuard: CanActivateFn = (_route, _state) => {
  const auth = inject(LoginService);
  const router = inject(Router);
  if (auth.isAuthenticated()) {
    return router.createUrlTree(['/']);
  }
  return true;
};