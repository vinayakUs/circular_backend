import { CommonModule } from '@angular/common';
import { Component, OnInit, inject, signal } from '@angular/core';
import { Router } from '@angular/router';

import { Persona, PersonasApiService } from '../services/personas-api.service';
import { CurrentPersonaService } from '../services/current-persona.service';
import { LoginService } from '../services/login.service';
import { personaIconPath } from '../util/persona-icons';

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
    return personaIconPath(iconName);
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