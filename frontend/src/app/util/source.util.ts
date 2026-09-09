/**
 * Canonical exchange/source identifiers used across the app for filtering.
 * The string values are what the backend (`/api/circulars`, `/bm25v2`,
 * `/hybrid`) accepts on its `source` parameter.
 */
export enum ExchangeSource {
  SEBI = 'SEBI',
  MASTER = 'SEBI_MASTER',
  NSE = 'NSE',
  ALL = 'ALL',
}

/**
 * Map a raw `source` token from the API to a human-readable display name.
 * Used by circular badges AND by the regulator/filter tabs in the UI, so any
 * change here shows up in both places.
 *
 * Falls back to the original value (or empty string) when no mapping exists.
 */
export function normalizeSource(source: string | null | undefined): string {
  switch (source?.toUpperCase()) {
    case 'SEBI':
      return 'SEBI';
    case 'NSE':
      return 'NSE';
    case 'SEBI_MASTER':
      return 'SEBI Master';
    case 'ALL':
      return 'All';
    default:
      return source ?? '';
  }
}

/**
 * Display label for a filter/regulator tab. Takes an `ExchangeSource` enum key
 * (e.g. `'MASTER'`) and returns the user-facing string (e.g. `'Sebi Master'`).
 * Routes through `normalizeSource` so all label logic lives in this file.
 */
export function exchangeLabel(key: keyof typeof ExchangeSource): string {
  return normalizeSource(ExchangeSource[key]);
}
