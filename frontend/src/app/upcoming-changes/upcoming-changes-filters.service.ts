import { Injectable, signal } from '@angular/core';

/** Period chip values the upcoming-changes page supports. */
export type UpcomingPeriod = '1W' | '2W' | '1M' | '2M' | '3M' | 'OTHER';

/**
 * Singleton store for the upcoming-changes page's filter state.
 *
 * Why this exists: filter state used to live in component-local signals.
 * When the user clicks a deadline and navigates to the circular detail
 * page, the component is destroyed, so coming back reset every chip to
 * defaults. A `providedIn: 'root'` service survives the component's
 * lifecycle — its signals stay populated — so the user's selections
 * come back intact when they return to the page.
 *
 * State is in-memory only. It does NOT survive a full page reload — by
 * design, that's a separate concern (URL params or session storage) and
 * wasn't part of the reported bug.
 */
@Injectable({ providedIn: 'root' })
export class UpcomingChangesFiltersService {
  // ── State (private, exposed via readonly signals) ────────────
  private _sourceFilter       = signal<string>('ALL');
  private _severityFilter     = signal<string>('ALL');
  private _periodFilter       = signal<UpcomingPeriod>('1M');
  private _customFromDate     = signal<string>('');
  private _customToDate       = signal<string>('');
  private _usePersonaFilter   = signal<boolean>(false);
  private _filtersExpanded    = signal<boolean>(false);

  readonly sourceFilter     = this._sourceFilter.asReadonly();
  readonly severityFilter   = this._severityFilter.asReadonly();
  readonly periodFilter     = this._periodFilter.asReadonly();
  readonly customFromDate   = this._customFromDate.asReadonly();
  readonly customToDate     = this._customToDate.asReadonly();
  readonly usePersonaFilter = this._usePersonaFilter.asReadonly();
  readonly filtersExpanded  = this._filtersExpanded.asReadonly();

  // ── Setters ──────────────────────────────────────────────────
  // Each is a no-op if the value is unchanged, so callers don't need
  // to guard for redundant writes before triggering a refetch.
  setSource(value: string): void {
    if (this._sourceFilter() === value) return;
    this._sourceFilter.set(value);
  }

  setSeverity(value: string): void {
    if (this._severityFilter() === value) return;
    this._severityFilter.set(value);
  }

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

  setUsePersonaFilter(value: boolean): void {
    if (this._usePersonaFilter() === value) return;
    this._usePersonaFilter.set(value);
  }

  toggleUsePersonaFilter(): void {
    this._usePersonaFilter.update((v) => !v);
  }

  toggleFiltersExpanded(): void {
    this._filtersExpanded.update((v) => !v);
  }
}
