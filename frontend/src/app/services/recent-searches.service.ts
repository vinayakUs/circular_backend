import { Injectable } from '@angular/core';

const STORAGE_KEY = 'allcirculars.recentSearches';
const MAX_RECENT = 8;

@Injectable({
  providedIn: 'root'
})
export class RecentSearchesService {
  /**
   * Reads recent searches from localStorage.
   * Returns an empty array if storage is unavailable or the value is malformed.
   */
  getRecent(): string[] {
    if (typeof window === 'undefined' || !window.localStorage) return [];
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .filter((v): v is string => typeof v === 'string' && v.trim().length > 0)
        .slice(0, MAX_RECENT);
    } catch {
      return [];
    }
  }

  /**
   * Adds a query to the front of the recent-searches list. De-duplicates
   * (case-insensitive) and caps the list at MAX_RECENT entries.
   */
  addRecent(query: string): string[] {
    const trimmed = (query ?? '').trim();
    if (!trimmed || typeof window === 'undefined' || !window.localStorage) {
      return this.getRecent();
    }
    const current = this.getRecent();
    const lower = trimmed.toLowerCase();
    const deduped = current.filter(item => item.toLowerCase() !== lower);
    const next = [trimmed, ...deduped].slice(0, MAX_RECENT);
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // Storage may be full or disabled — keep the in-memory list and move on.
    }
    return next;
  }

  /**
   * Removes a single query from the recent-searches list.
   */
  removeRecent(query: string): string[] {
    if (typeof window === 'undefined' || !window.localStorage) return [];
    const current = this.getRecent();
    const next = current.filter(item => item !== query);
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