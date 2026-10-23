import { HttpClient } from '@angular/common/http';
import { Injectable, signal } from '@angular/core';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';

import { environment } from '../../environments/environment';
import { Persona } from './personas-api.service';
import { User } from './auth.service';

/**
 * Reactive store for the current user's selected persona.
 *
 * Single source of truth: the Angular signal, populated from a User the
 * caller hands in. This service knows nothing about authentication.
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

  constructor(private http: HttpClient) {}

  /** Call on logout so the next login doesn't see the previous user's persona. */
  reset(): void {
    this._slug.set(null);
    this._persona.set(null);
    this._loaded.set(false);
  }

  /** Adopt the persona carried by a /api/auth/session or /mfa/verify body. */
  setFromUser(user: User | null): void {
    this._slug.set(user?.persona_slug ?? null);
    this._persona.set(user?.persona ?? null);
    this._loaded.set(true);
  }

  /**
   * Persist a persona choice. The slug updates optimistically so the UI
   * reacts immediately; the full object is resolved from /api/personas.
   */
  set(slug: string | null): Observable<void> {
    return this.http
      .put<void>(`${environment.apiUrl}/api/auth/me/persona`, { slug })
      .pipe(
        tap(() => {
          this._slug.set(slug);
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
        this._persona.set(list.find((p) => p.slug === slug) ?? null);
      });
  }
}
