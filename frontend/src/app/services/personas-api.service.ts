import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, map } from 'rxjs';

import { environment } from '../../environments/environment';
import { DeadlineEvent } from '../shared/deadline-calendar/deadline-calendar.component';

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
   * `priority` and `exchange` accept a single value or a CSV. */
  getDeadlines(
    options?: {
      from?: string;
      to?: string;
      limit?: number;
      offset?: number;
      priority?: string;
      exchange?: string;
    }
  ): Observable<DeadlineEvent[]> {
    const params = new URLSearchParams();
    if (options?.from)     params.set('from',     options.from);
    if (options?.to)       params.set('to',       options.to);
    if (options?.limit  !== undefined) params.set('limit',  options.limit.toString());
    if (options?.offset !== undefined) params.set('offset', options.offset.toString());
    if (options?.priority) params.set('priority', options.priority);
    if (options?.exchange) params.set('exchange', options.exchange);
    const qs = params.toString();
    return this.http
      .get<{ items: DeadlineEvent[] }>(
        `${environment.apiUrl}/api/deadlines${qs ? '?' + qs : ''}`
      )
      .pipe(map((r: { items: DeadlineEvent[] }) => r.items ?? []));
  }
}