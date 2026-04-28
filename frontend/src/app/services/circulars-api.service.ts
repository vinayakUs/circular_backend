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
  source: string;
  circular_id: string;
  full_reference: string;
  department: string;
  title: string;
  issue_date: string;
  effective_date: string;
  status: string;
  url: string;
}

export interface PaginatedCircularsResponse {
  data: {
    circulars: Circular[];
  };
  pagination: {
    limit: number;
    offset: number;
    total: number;
    hasNext: boolean;
    hasPrev: boolean;
  };
}

export interface CircularsResponse {
  items: Circular[];
  total: number;
}

export interface SemanticSearchResponse {
  query: string;
  strategy: string;
  answer: string;
  references: {
    circular_id: string;
    relevance_score: number;
    source: string;
    title: string;
    url: string;
  }[];
  snippets: string[];
  rag_error?: string;
}

export interface SearchResponse {
  query: string;
  strategy: string;
  results: SearchResult[];
}

export interface SearchResult {
  id: string;
  score: number;
  chunkId: string;
  circularId: string;
  fullReference: string;
  department: string;
  source: string;
  title: string;
  issueDate: string;
  url: string;
  chunkIndex: number;
  preview: string;
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

  getLatestCirculars(): Observable<PaginatedCircularsResponse> {
    const url = `${this.baseUrl}/api/circulars?source=ALL&limit=4&offset=0`;
    return this.http.get<PaginatedCircularsResponse>(url).pipe(
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
  }): Observable<PaginatedCircularsResponse> {
    const queryParams = new URLSearchParams();
    if (params.source) queryParams.set('source', params.source);
    if (params.limit) queryParams.set('limit', params.limit.toString());
    if (params.offset !== undefined) queryParams.set('offset', params.offset.toString());
    if (params.from_date) queryParams.set('from_date', params.from_date);
    if (params.to_date) queryParams.set('to_date', params.to_date);
    if (params.search) queryParams.set('search', params.search);

    const url = `${this.baseUrl}/api/circulars?${queryParams.toString()}`;
    return this.http.get<PaginatedCircularsResponse>(url);
  }

  semanticSearch(params: {
    query: string;
    strategy?: string;
    source?: string;
    from_date?: string;
    to_date?: string;
  }): Observable<SemanticSearchResponse> {
    return this.http.post<SemanticSearchResponse>(
      `${this.baseUrl}/api/circulars/search/hybrid`,
      { q: params.query, source: params.source, from_date: params.from_date, to_date: params.to_date }
    );
  }

  keywordSearch(params: {
    query: string;
    source?: string;
    from_date?: string;
    to_date?: string;
  }): Observable<SearchResponse> {
    return this.http.post<SearchResponse>(
      `${this.baseUrl}/api/circulars/search/bm25`,
      { q: params.query, source: params.source, from_date: params.from_date, to_date: params.to_date }
    );
  }
}