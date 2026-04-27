import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, of, tap, throwError } from 'rxjs';

export interface CountsResponse {
  total: number;
  sebi: number;
  nse: number;
}

export interface Circular {
  id: string;
  circular_id: string;
  full_reference: string;
  issue_date: string;
  title: string;
  source: string;
  url: string;
}

export interface CircularsResponse {
  items: Circular[];
  total: number;
}

export interface SemanticSearchResponse {
  answer: string;
  query: string;
  references: {
    circular_id: string;
    relevance_score: number;
    source: string;
    title: string;
    url: string;
  }[];
  snippets: string[];
  strategy: string;
}

export interface SearchResponse {
  query: string;
  results: SearchResult[];
  strategy: string;
}

export interface SearchResult {
  document: Circular;
  id: string;
  preview: string;
  score: number;
}

@Injectable({ providedIn: 'root' })
export class CircularsApiService {
  private http = inject(HttpClient);
  private baseUrl = 'http://127.0.0.1:5000';
  private countsCacheKey = 'circulars_counts';
  private listCacheKey = 'circulars_list';

  getCounts(): Observable<CountsResponse> {
    return this.http.get<CountsResponse>(
      `${this.baseUrl}/api/circulars/counts`
    ).pipe(
      tap(data => localStorage.setItem(this.countsCacheKey, JSON.stringify(data))),
      catchError(() => {
        const cached = localStorage.getItem(this.countsCacheKey);
        return cached ? of(JSON.parse(cached)) : throwError(() => new Error('No cache'));
      })
    );
  }

  getLatestCirculars(): Observable<CircularsResponse> {
    const url = `${this.baseUrl}/api/circulars?source=ALL&limit=4&offset=0`;
    return this.http.get<CircularsResponse>(url).pipe(
      tap(data => localStorage.setItem(this.listCacheKey, JSON.stringify(data))),
      catchError(() => {
        const cached = localStorage.getItem(this.listCacheKey);
        return cached ? of(JSON.parse(cached)) : throwError(() => new Error('No cache'));
      })
    );
  }

  getCirculars(params: {
    source?: string;
    limit?: number;
    offset?: number;
    from_date?: string;
    to_date?: string;
    search?: string;
  }): Observable<CircularsResponse> {
    const queryParams = new URLSearchParams();
    if (params.source) queryParams.set('source', params.source);
    if (params.limit) queryParams.set('limit', params.limit.toString());
    if (params.offset !== undefined) queryParams.set('offset', params.offset.toString());
    if (params.from_date) queryParams.set('from_date', params.from_date);
    if (params.to_date) queryParams.set('to_date', params.to_date);
    if (params.search) queryParams.set('search', params.search);

    const url = `${this.baseUrl}/api/circulars?${queryParams.toString()}`;
    const cacheKey = `circulars_list_${params.source}_${params.offset}_${params.search || ''}`;

    return this.http.get<CircularsResponse>(url).pipe(
      tap(data => localStorage.setItem(cacheKey, JSON.stringify(data))),
      catchError(() => {
        const cached = localStorage.getItem(cacheKey);
        return cached ? of(JSON.parse(cached)) : of({ items: [], total: 0 });
      })
    );
  }

  semanticSearch(params: {
    query: string;
    strategy?: string;
    source?: string;
    from_date?: string;
    to_date?: string;
  }): Observable<SemanticSearchResponse> {
    return this.http.post<SemanticSearchResponse>(
      `${this.baseUrl}/api/circulars/search`,
      { q: params.query, ...params }
    );
  }

  keywordSearch(params: {
    query: string;
    source?: string;
    from_date?: string;
    to_date?: string;
  }): Observable<SearchResponse> {
    const { query, ...rest } = params;
    return this.http.post<SearchResponse>(
      `${this.baseUrl}/api/circulars/search`,
      { q: query, strategy: 'bm25', ...rest }
    );
  }
}