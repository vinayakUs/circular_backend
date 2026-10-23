import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Router } from '@angular/router';
import { finalize } from 'rxjs';

import { environment } from 'src/environments/environment';

import { AuthService } from './auth.service';
import { CurrentPersonaService } from './current-persona.service';

const CSRF_HEADER = 'X-CSRF-Token';

@Injectable({ providedIn: 'root' })
export class LogoutService {
  private baseUrl = environment.apiUrl;

  private http = inject(HttpClient);
  private router = inject(Router);
  private auth = inject(AuthService);
  private personaSvc = inject(CurrentPersonaService);

  /**
   * End the session.
   *
   * This MUST round-trip: the access and refresh tokens are HttpOnly, so
   * JS cannot delete them and only the server can purge the refresh-token
   * family. Local cleanup runs regardless of how the call ends — a user who
   * clicks logout must reach the redirect target even if the network is down.
   */
  logout(redirectTo: string = '/'): void {
    const csrf = this.auth.csrf();

    this.http
      .post(`${this.baseUrl}/api/auth/logout`, null, {
        withCredentials: true,
        headers: csrf ? { [CSRF_HEADER]: csrf } : {},
      })
      .pipe(finalize(() => this.finishLocally(redirectTo)))
      // finalize() already ran by the time RxJS reports the unhandled error;
      // this handler only stops it surfacing as an unhandled rejection.
      .subscribe({ error: () => undefined });
  }

  private finishLocally(redirectTo: string): void {
    this.auth.reset();
    this.personaSvc.reset();
    this.router.navigate([redirectTo]);
  }
}
