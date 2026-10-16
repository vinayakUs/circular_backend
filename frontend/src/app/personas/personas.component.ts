import { CommonModule } from '@angular/common';
import { Component, OnInit, inject, signal } from '@angular/core';
import { Router } from '@angular/router';

import { Persona, PersonasApiService } from '../services/personas-api.service';
import { CurrentPersonaService } from '../services/current-persona.service';
import { LoginService } from '../services/login.service';

/** Map of icon-name (from backend) → SVG path data. */
const ICON_PATHS: Record<string, string> = {
  'shield-check':
    'M12 2L4 5v6c0 5 3.5 9.5 8 11 4.5-1.5 8-6 8-11V5l-8-3zm-1 12l-3-3 1.4-1.4L11 11.2l4.6-4.6L17 8l-6 6z',
  'bar-chart-2': 'M3 3v18h18M7 14l4-4 4 4 5-5',
  'alert-triangle':
    'M12 2L1 21h22L12 2zm0 6l7.5 13h-15L12 8zm-1 4v4h2v-4h-2zm0 6v2h2v-2h-2z',
  'cpu':
    'M20 3H4c-1.1 0-2 .9-2 2v11c0 1.1.9 2 2 2h7l-2 2v1h6v-1l-2-2h7c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 13H4V5h16v11z',
  'settings':
    'M19.14 12.94c.04-.31.06-.63.06-.94 0-.31-.02-.63-.06-.94l2.03-1.58a.49.49 0 00.12-.61l-1.92-3.32a.488.488 0 00-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54A.5.5 0 0014 2h-4a.5.5 0 00-.49.42l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96a.49.49 0 00-.59.22L2.63 8.48a.49.49 0 00.12.61l2.03 1.58c-.04.31-.06.63-.06.94s.02.63.06.94l-2.03 1.58a.49.49 0 00-.12.61l1.92 3.32c.13.22.39.31.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.26.5.5.5h4a.5.5 0 00.49-.42l.36-2.54c.59-.24 1.13-.57 1.62-.94l2.39.96c.22.09.46 0 .59-.22l1.92-3.32a.49.49 0 00-.12-.61l-2.03-1.58zM12 15.5A3.5 3.5 0 1115.5 12 3.5 3.5 0 0112 15.5z',
  'globe':
    'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z',
  'trending-up':
    'M16 6l2.29 2.29-4.88 4.88-4-4L2 16.59 3.41 18l6-6 4 4 6.3-6.29L22 12V6h-6z',
};

const DEFAULT_ICON =
  'M12 2L4 5v6c0 5 3.5 9.5 8 11 4.5-1.5 8-6 8-11V5l-8-3z';

@Component({
  selector: 'app-personas',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './personas.component.html',
  styleUrl: './personas.component.css',
})
export class PersonasComponent implements OnInit {
  personas: Persona[] = [];
  loading = true;
  error: string | null = null;

  /** Slug currently being persisted to the backend for an in-flight card click. */
  readonly savingSlug = signal<string | null>(null);

  /** Slug of the card whose latest save attempt failed (cleared on next attempt). */
  readonly savingErrorSlug = signal<string | null>(null);

  private api = inject(PersonasApiService);
  private personaSvc = inject(CurrentPersonaService);
  private auth = inject(LoginService);
  private router = inject(Router);

  ngOnInit() {
    this.api.getPersonas().subscribe({
      next: (data) => {
        this.personas = data;
        this.loading = false;
      },
      error: () => {
        this.error = 'Could not load personas.';
        this.loading = false;
      },
    });
  }

  /** True if `slug` matches the user's currently selected persona.
   *
   * Requires the user to be authenticated — the auth interceptor clears
   * persona state on a 401, but the `/personas` route is public and
   * `getPersonas()` is unauthenticated, so a leftover in-memory slug
   * (from a session that ended without triggering a protected request)
   * would otherwise still mark a card as "Current" for logged-out visitors. */
  isCurrentPersona(slug: string): boolean {
    return this.auth.isAuthenticated() && this.personaSvc.slug() === slug;
  }

  /** True if the user is mid-authenticated (i.e. their selection would persist). */
  get isAuthenticated(): boolean {
    return this.auth.isAuthenticated();
  }

  /** Map backend-provided icon name to an inline SVG path. */
  iconPath(iconName: string): string {
    return ICON_PATHS[iconName] ?? DEFAULT_ICON;
  }

  /** Unique source list per persona for the chip row. */
  uniqueSources(persona: Persona): string[] {
    const seen = new Set<string>();
    for (const f of persona.filters) seen.add(f.source);
    return Array.from(seen);
  }

  /**
   * Card click handler. Three branches:
   *   1. Unauthenticated → just navigate, no backend write (existing behavior).
   *   2. Authenticated, same persona as current → no-op PUT, just navigate.
   *   3. Authenticated, different persona → PUT, navigate on success, show
   *      inline error on failure. State (signal, localStorage) updates ONLY
   *      happen on backend success inside CurrentPersonaService.set().
   *
   * Power-user clicks (modifier keys / middle button / non-zero button) are
   * left alone so browsers can do "open in new tab" naturally.
   */
  selectPersona(p: Persona, event: MouseEvent): void {
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.button !== 0) {
      return;
    }
    event.preventDefault();

    if (!this.isAuthenticated) {
      this._navigateToDetail(p.slug);
      return;
    }

    if (this.isCurrentPersona(p.slug)) {
      this._navigateToDetail(p.slug);
      return;
    }

    this.savingErrorSlug.set(null);
    this.savingSlug.set(p.slug);

    this.personaSvc.set(p.slug).subscribe({
      next: () => {
        this.savingSlug.set(null);
        this._navigateToDetail(p.slug);
      },
      error: (err) => {
        this.savingSlug.set(null);
        this.savingErrorSlug.set(p.slug);
        console.error('Persona save failed', err);
      },
    });
  }

  private _navigateToDetail(slug: string): void {
    this.router.navigate(['/personas', slug]);
  }
}