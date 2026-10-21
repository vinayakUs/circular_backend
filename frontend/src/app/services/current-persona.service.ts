import { HttpClient } from '@angular/common/http';
import { Injectable, signal } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';

import { environment } from '../../environments/environment';
import { Persona } from './personas-api.service';
import { LoginService } from './login.service';

/**
 * Reactive store for the current user's selected persona.
 *
 * Two sources of truth, kept consistent:
 *   - Angular signal (reactive, in-memory, shared with all components)
 *   - localStorage 'user_details' (durable, survives reloads)
 *
 * On PUT, both update in the same handler so they can't drift visibly
 * to the user. Bootstrap prefers localStorage (zero HTTP); only login
 * (post-MFA) fetches /me.
 */
@Injectable({ providedIn: 'root' })
export class CurrentPersonaService {
  // ── State ───────────────────────────────────────────────
  private _slug = signal<string | null>(null);
  private _persona = signal<Persona | null>(null);
  private _loaded = signal(false);

  readonly slug = this._slug.asReadonly();
  readonly persona = this._persona.asReadonly();
  readonly loaded = this._loaded.asReadonly();

  constructor(
    private loginSvc: LoginService,
    private http: HttpClient,
  ) {}

  hydrateFromStorage(): void {
    const u = this.loginSvc.getStoredUser();
    if (u) {
      this._slug.set(u.persona_slug ?? null);
      this._persona.set(u.persona ?? null);
    }
    this._loaded.set(true);
  }

  /** Call on logout so the next login doesn't see the previous user's persona. */
  reset(): void {
    this._slug.set(null);
    this._persona.set(null);
    this._loaded.set(false);
  }

  setFromMe(user: {
    persona_slug?: string | null;
    persona?: Persona | null;
  }): void {
    this._slug.set(user.persona_slug ?? null);
    this._persona.set(user.persona ?? null);
    this._loaded.set(true);
  }

  /**
   * The slug is mirrored to localStorage immediately on PUT success so a
   * later follow-up GET failure (resolving the full persona object) can't
   * leave the durable cache stale. When /api/personas resolves, we
   * overwrite the entry with the full object.
   */
  set(slug: string | null): Observable<void> {
    return this.http
      .put<void>(`${environment.apiUrl}/api/auth/me/persona`, { slug })
      .pipe(
        tap(() => {
          this._slug.set(slug);
          this._mirrorToLocalStorage(slug, null);
          if (slug) {
            this._resolvePersonaFromConfig(slug);
          } else {
            this._persona.set(null);
          }
        }),
      );
  }

  private _resolvePersonaFromConfig(slug: string): void {
    this.http
      .get<Persona[]>(`${environment.apiUrl}/api/personas`)
      .subscribe((list) => {
        const match = list.find((p) => p.slug === slug) ?? null;
        this._persona.set(match);
        this._mirrorToLocalStorage(slug, match);
      });
  }

  /** Skipped if there's no stored user (anonymous session). */
  private _mirrorToLocalStorage(
    slug: string | null,
    persona: Persona | null,
  ): void {
    const stored = this.loginSvc.getStoredUser();
    if (!stored) return;
    stored.persona_slug = slug;
    stored.persona = persona;
    localStorage.setItem('user_details', JSON.stringify(stored));
  }
}