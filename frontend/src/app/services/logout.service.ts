import { Injectable, inject } from '@angular/core';
import { Router } from '@angular/router';

import { CurrentPersonaService } from './current-persona.service';

/**
 * Single place to clean up all client-side auth state on logout.
 *
 * Order of operations matters:
 *   1. Cookie  — clear JWT so subsequent /me calls (if any) 401
 *   2. Storage — clear localStorage so next login starts fresh
 *   3. Signals — reset CurrentPersonaService so the UI doesn't briefly
 *                show the previous user's persona
 *   4. Navigate — last; components that read auth state in ngOnDestroy
 *                still see a valid state until navigation completes
 */
@Injectable({ providedIn: 'root' })
export class LogoutService {
  private router = inject(Router);
  private personaSvc = inject(CurrentPersonaService);

  logout(redirectTo: string = '/'): void {
    document.cookie =
      'access_token=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT';
    localStorage.removeItem('user_details');
    this.personaSvc.reset();
    this.router.navigate([redirectTo]);
  }
}