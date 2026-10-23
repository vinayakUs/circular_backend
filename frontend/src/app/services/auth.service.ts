import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable, NgZone, inject, signal } from '@angular/core';
import {
  Observable,
  catchError,
  finalize,
  of,
  shareReplay,
  tap,
  throwError,
} from 'rxjs';

import { environment } from 'src/environments/environment';

import { Persona } from './personas-api.service';

/** The authenticated user. */
export interface User {
  username: string;
  user_db_id?: string;
  department_id?: string;
  department?: string | null;
  email?: string | null;
  name?: string | null;
  persona_slug?: string | null;
  persona?: Persona | null;
}

/** Session envelope returned by the server. */
export interface SessionResponse {
  authenticated: boolean;
  refreshed: boolean;
  access_expires_at: string;
  csrf_token: string;
  user: User;
}

/**
 * 'unknown' is load-bearing: it means "not asked yet" and is distinct from
 * 'anonymous'. Redirecting on 'unknown' would bounce a signed-in user out
 * on a cold load, before the session call has resolved.
 */
export type AuthStatus = 'unknown' | 'authenticated' | 'anonymous';

/** Renew this long before the access token dies, when there's room. */
const RENEWAL_LEAD_MS = 30_000;
/** Never schedule sooner than this, so a timer can't spin. */
const RENEWAL_MIN_DELAY_MS = 1_000;
/** Back-off after the renewal call itself failed (network, 5xx). */
const RENEWAL_RETRY_MS = 15_000;

@Injectable({ providedIn: 'root' })
export class AuthService {
  private http = inject(HttpClient);
  private zone = inject(NgZone);
  private baseUrl = environment.apiUrl;

  private _status = signal<AuthStatus>('unknown');
  private _user = signal<User | null>(null);
  private _csrf = signal<string | null>(null);

  readonly status = this._status.asReadonly();
  readonly user = this._user.asReadonly();
  readonly csrf = this._csrf.asReadonly();

  /**
   * Access-token expiry as epoch ms, 0 when unknown. This is what makes
   * auth state time-aware: without it an idle tab keeps reporting
   * 'authenticated' long after the token died, since nothing invalidates
   * the status until some request happens to 401.
   */
  private accessExpiresAt = 0;
  private renewalTimer: ReturnType<typeof setTimeout> | null = null;

  private inflight: Observable<SessionResponse | null> | null = null;

  /**
   * The single source of truth for auth state. With HttpOnly cookies the
   * SPA cannot read the access token, so it must ask the server.
   */
  session(): Observable<SessionResponse | null> {
    return this.http
      .get<SessionResponse>(`${this.baseUrl}/api/auth/session`, {
        withCredentials: true,
      })
      .pipe(
        catchError((err) => {
          if (err instanceof HttpErrorResponse && err.status === 401) {
            return of(null);
          }
          // A network error or 5xx is NOT 'anonymous'. Rethrow so the
          // status stays 'unknown' rather than ejecting a user whose
          // session is merely unreachable.
          throw err;
        }),
        tap((s) => this.apply(s)),
      );
  }

  /**
   * Single-flight session(): parallel requests on an expired access token
   * must rotate once, not N times. Not for the guards — a guard runs once
   * per navigation, and sharing there would replay one guard's result
   * against unrelated requests.
   */
  refreshOnce(): Observable<SessionResponse | null> {
    if (!this.inflight) {
      this.inflight = this.session().pipe(
        shareReplay({ bufferSize: 1, refCount: false }),
        finalize(() => {
          this.inflight = null;
        }),
      );
    }
    return this.inflight;
  }

  /** Adopt a session obtained during login. */
  adopt(session: SessionResponse): void {
    this.apply(session);
  }

  /** Local state only; ending the session server-side is a separate call. */
  reset(): void {
    this.apply(null);
  }

  /**
   * Time-aware on purpose: an expired access token means the UI must stop
   * claiming the user is signed in, or the navbar keeps showing their name
   * on a session the server would now reject.
   */
  isAuthenticated(): boolean {
    return this._status() === 'authenticated' && !this.expired();
  }

  /** True once the access token has passed its expiry. */
  expired(): boolean {
    return this.accessExpiresAt > 0 && Date.now() >= this.accessExpiresAt;
  }

  private apply(s: SessionResponse | null): void {
    this.clearRenewal();

    if (s) {
      this._user.set(s.user);
      this._csrf.set(s.csrf_token);
      this.accessExpiresAt = this.parseExpiry(s.access_expires_at);
      this._status.set('authenticated');
      this.scheduleRenewal();
    } else {
      this._user.set(null);
      this._csrf.set(null);
      this.accessExpiresAt = 0;
      this._status.set('anonymous');
    }
  }

  /**
   * Renew shortly before the access token dies, so an active tab is
   * silently renewed instead of waiting to fail a request and recover.
   *
   * Timed outside the Angular zone: a zone-patched setTimeout wakes change
   * detection on every tick for no reason.
   */
  private scheduleRenewal(): void {
    const remaining = this.accessExpiresAt - Date.now();
    const lead = Math.max(0, Math.min(RENEWAL_LEAD_MS, remaining * 0.1));
    const delay = Math.max(RENEWAL_MIN_DELAY_MS, lead);

    this.zone.runOutsideAngular(() => {
      this.renewalTimer = setTimeout(() => this.renew(), delay);
    });
  }

  private renew(): void {
    this.renewalTimer = null;
    this.refreshOnce().subscribe({
      // apply() already rescheduled on success and cleared on an anonymous
      // result, so neither branch has anything left to do.
      next: () => undefined,
      // The server was unreachable, which is not a logout — retry soon.
      // Once the token is genuinely dead isAuthenticated() goes false and
      // the interceptor's reactive path takes over.
      error: () => {
        this.zone.runOutsideAngular(() => {
          this.renewalTimer = setTimeout(() => this.renew(), RENEWAL_RETRY_MS);
        });
      },
    });
  }

  private clearRenewal(): void {
    if (this.renewalTimer !== null) {
      clearTimeout(this.renewalTimer);
      this.renewalTimer = null;
    }
  }

  /**
   * Python's isoformat() emits six fractional digits while Date.parse is
   * only specified to accept three. V8 is lenient and Safari has not
   * always been — a NaN here would silently disable the expiry check.
   */
  private parseExpiry(iso: string): number {
    if (!iso) return 0;
    const ms = Date.parse(iso);
    if (!Number.isNaN(ms)) return ms;
    const trimmed = Date.parse(iso.replace(/(\.\d{3})\d+/, '$1'));
    return Number.isNaN(trimmed) ? 0 : trimmed;
  }
}
