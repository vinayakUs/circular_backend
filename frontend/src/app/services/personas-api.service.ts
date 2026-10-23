import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, map } from 'rxjs';

import { environment } from '../../environments/environment';
import { DeadlineEvent } from '../shared/deadline-calendar/deadline-calendar.component';
import { PaginatedCircularsResponse } from './circulars-api.service';
import { appendSources } from '../util/source.util';

export interface PersonaFilter {
  source: string;
  departments: string[];
  categories: string[];
}

export interface Persona {
  slug: string;
  name: string;
  description: string;
  icon: string;
  filters: PersonaFilter[];
  keywords: string[];
}

export type { DeadlineEvent };

@Injectable({ providedIn: 'root' })
export class PersonasApiService {
  constructor(private http: HttpClient) {}

  getPersonas(): Observable<Persona[]> {
    return this.http.get<Persona[]>(`${environment.apiUrl}/api/personas`);
  }

  /** Backend defaults to today-30 → today+335 if no `from`/`to` is passed. */
  getDeadlinesForPersona(
    slug: string,
    options?: { from?: string; to?: string; limit?: number; offset?: number; exchange?: string }
  ): Observable<DeadlineEvent[]> {
    let params = new URLSearchParams();
    if (options?.from)   params.set('from',   options.from);
    if (options?.to)     params.set('to',     options.to);
    if (options?.limit  !== undefined) params.set('limit',  options.limit.toString());
    if (options?.offset !== undefined) params.set('offset', options.offset.toString());
    if (options?.exchange) params.set('exchange',options.exchange);
    const qs = params.toString();
    return this.http
      .get<{ items: DeadlineEvent[] }>(
        `${environment.apiUrl}/api/personas/${slug}/deadlines${qs ? '?' + qs : ''}`
      )
      .pipe(map((r: { items: DeadlineEvent[] }) => r.items ?? []));
  }

  /** Backend defaults to today → today+60d if no `from`/`to` is passed.
   * `exchange` accepts a single value or a CSV. */
  getDeadlines(
    options?: {
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
      exchange?: string;
    }
  ): Observable<DeadlineEvent[]> {
    const params = new URLSearchParams();
    if (options?.from)     params.set('from',     options.from);
    if (options?.to)       params.set('to',       options.to);
    if (options?.limit  !== undefined) params.set('limit',  options.limit.toString());
    if (options?.offset !== undefined) params.set('offset', options.offset.toString());
    if (options?.exchange) params.set('exchange', options.exchange);
    const qs = params.toString();
    return this.http
      .get<{ items: DeadlineEvent[] }>(
        `${environment.apiUrl}/api/deadlines${qs ? '?' + qs : ''}`
      )
      .pipe(map((r: { items: DeadlineEvent[] }) => r.items ?? []));
  }

  /**
   * Circulars matched to a persona, newest first.
   *
   * Shares `/api/circulars`' response envelope, so the existing `Circular`
   * interface applies unchanged — callers only add `normalizedSource` via
   * `normalizeSource(source)` for display.
   *
   * `source` accepts a single value or an array and is sent as repeated
   * params; the UI-only `ALL` sentinel is dropped by `appendSources`, so an
   * absent param is how "no source filter" reaches the API.
   */
  getCircularsForPersona(
    slug: string,
    options?: {
      from?: string;
      to?: string;
      source?: string | string[];
      limit?: number;
      offset?: number;
    }
  ): Observable<PaginatedCircularsResponse> {
    const params = new URLSearchParams();
    if (options?.from)      params.set('from', options.from);
    if (options?.to)        params.set('to', options.to);
    appendSources(params, options?.source);
    if (options?.limit  !== undefined) params.set('limit',  options.limit.toString());
    if (options?.offset !== undefined) params.set('offset', options.offset.toString());
    const qs = params.toString();
    return this.http.get<PaginatedCircularsResponse>(
      `${environment.apiUrl}/api/personas/${slug}/circulars${qs ? '?' + qs : ''}`
    );
  }
}
