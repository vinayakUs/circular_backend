import { Injectable, signal } from '@angular/core';

import { UpcomingPeriod } from '../upcoming-changes/upcoming-changes-filters.service';

/**
 * Singleton store for the persona page's period filter.
 *
 * Why a root service rather than component signals: clicking a circular
 * navigates to the detail page, which destroys PersonaDetailComponent and
 * would reset every chip on the way back. Same reasoning as
 * UpcomingChangesFiltersService.
 *
 * Source is deliberately NOT held here — it stays on the component so the
 * page's single source chip row drives the calendar, the upcoming-deadlines
 * widget and the circulars list together.
 *
 * State is in-memory only; a full page reload resets it. That is a separate
 * concern (URL params or session storage) and was not part of the ask.
 *
 * The `UpcomingPeriod` union is shared with the upcoming-changes page but
 * means the opposite thing on each: here the windows are backward-looking
 * ("last 1 month" over issue_date), there they are forward-looking over
 * future deadlines. The component's PERIOD_DAYS map is typed
 * `Record<Exclude<UpcomingPeriod, 'OTHER'>, number>`, so adding a chip to that
 * union without a mapping is a compile error rather than a runtime hole.
 */
@Injectable({ providedIn: 'root' })
export class PersonaDetailFiltersService {
  // ── State (private, exposed via readonly signals) ────────────
  private _periodFilter   = signal<UpcomingPeriod>('1M');
  private _customFromDate = signal<string>('');
  private _customToDate   = signal<string>('');
  private _activeSlug     = signal<string | null>(null);

  readonly periodFilter    = this._periodFilter.asReadonly();
  readonly customFromDate  = this._customFromDate.asReadonly();
  readonly customToDate    = this._customToDate.asReadonly();
  readonly activeSlug      = this._activeSlug.asReadonly();

  // ── Setters ──────────────────────────────────────────────────
  // Each is a no-op if the value is unchanged, so callers don't need to guard
  // for redundant writes before triggering a refetch.
  setPeriod(value: UpcomingPeriod): void {
    if (this._periodFilter() === value) return;
    this._periodFilter.set(value);
  }

  setCustomDate(which: 'from' | 'to', value: string): void {
    if (which === 'from') {
      if (this._customFromDate() === value) return;
      this._customFromDate.set(value);
    } else {
      if (this._customToDate() === value) return;
      this._customToDate.set(value);
    }
  }

  setActiveSlug(value: string | null): void {
    if (this._activeSlug() === value) return;
    this._activeSlug.set(value);
  }

  /**
   * Restore defaults. Called when the route slug differs from the one the
   * retained filters were set under, so moving between personas starts clean
   * while a round-trip through the circular detail page preserves the user's
   * selections.
   */
  resetToDefaults(): void {
    this._periodFilter.set('1M');
    this._customFromDate.set('');
    this._customToDate.set('');
  }
}
