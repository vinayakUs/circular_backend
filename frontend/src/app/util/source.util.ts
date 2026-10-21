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

/**
 * Append `source` params for the circulars endpoints.
 *
 * `ExchangeSource.ALL` is a UI-only sentinel (the "All" regulator tab) — the
 * API has no ALL member, so omitting the param is how "no source filter" is
 * expressed. Sending `source=ALL` would be rejected.
 */
export function appendSources(
  queryParams: URLSearchParams,
  source?: string | string[]
): void {
  const values = (Array.isArray(source) ? source : [source])
    .filter((s): s is string => !!s && s !== ExchangeSource.ALL);
  values.forEach(s => queryParams.append('source', s));
}
