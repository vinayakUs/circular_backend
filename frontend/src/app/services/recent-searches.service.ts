import { Injectable } from '@angular/core';

// Historical key name — recent searches used to live on the allcirculars page
// before the search bar moved to /keyword-search. Kept as-is so existing
// users don't lose their history.
const STORAGE_KEY = 'allcirculars.recentSearches';
const MAX_RECENT = 8;

export type SearchMode = 'keyword' | 'semantic' | 'phrase' | 'title';

export interface RecentSearch {
  q: string;
  mode: SearchMode;
}

function isMode(value: unknown): value is SearchMode {
  return (
    value === 'keyword'
    || value === 'semantic'
    || value === 'phrase'
    || value === 'title'
  );
}

/**
 * Normalizes one stored entry.
 *
 * Entries were originally persisted as bare strings, with no mode. Those are
 * migrated on read and assumed to be keyword searches, which is what the old
 * allcirculars bar always sent (`mode: 'keyword'`).
 */
function toRecentSearch(raw: unknown): RecentSearch | null {
  if (typeof raw === 'string') {
    const q = raw.trim();
    return q ? { q, mode: 'keyword' } : null;
  }
  if (raw && typeof raw === 'object') {
    const { q, mode } = raw as { q?: unknown; mode?: unknown };
    if (typeof q === 'string' && q.trim()) {
      return { q: q.trim(), mode: isMode(mode) ? mode : 'keyword' };
    }
  }
  return null;
}

@Injectable({
  providedIn: 'root'
})
export class RecentSearchesService {
  /**
   * Reads recent searches from localStorage.
   * Returns an empty array if storage is unavailable or the value is malformed.
   */
  getRecent(): RecentSearch[] {
    if (typeof window === 'undefined' || !window.localStorage) return [];
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map(toRecentSearch)
        .filter((v): v is RecentSearch => v !== null)
        .slice(0, MAX_RECENT);
    } catch {
      return [];
    }
  }

  /**
   * Adds a query to the front of the recent-searches list. Caps the list at
   * MAX_RECENT entries.
   *
   * De-duplication is on query *and* mode (query case-insensitively), so the
   * same text searched both ways keeps two entries — they return different
   * results, so both are worth getting back to.
   */
  addRecent(query: string, mode: SearchMode): RecentSearch[] {
    const trimmed = (query ?? '').trim();
    if (!trimmed || typeof window === 'undefined' || !window.localStorage) {
      return this.getRecent();
    }
    const current = this.getRecent();
    const lower = trimmed.toLowerCase();
    const deduped = current.filter(
      item => !(item.q.toLowerCase() === lower && item.mode === mode)
    );
    const next = [{ q: trimmed, mode }, ...deduped].slice(0, MAX_RECENT);
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // Storage may be full or disabled — keep the in-memory list and move on.
    }
    return next;
  }

  /**
   * Removes a single entry. Matches on both query and mode so removing the
   * keyword variant leaves the semantic one alone.
   */
  removeRecent(query: string, mode: SearchMode): RecentSearch[] {
    if (typeof window === 'undefined' || !window.localStorage) return [];
    const current = this.getRecent();
    const next = current.filter(item => !(item.q === query && item.mode === mode));
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // ignore — see addRecent
    }
    return next;
  }

  /**
   * Clears all recent searches.
   */
  clearRecent(): void {
    if (typeof window === 'undefined' || !window.localStorage) return;
    try {
      window.localStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
  }
}
