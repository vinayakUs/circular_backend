import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of, throwError } from 'rxjs';

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
  private baseUrl = '';
  getCounts(): Observable<CountsResponse> {
    return this.http.get<CountsResponse>(
      `${this.baseUrl}/api/circulars/counts`
    );
  }

  getLatestCirculars(): Observable<PaginatedCircularsResponse> {
    const url = `${this.baseUrl}/api/circulars?source=ALL&limit=4&offset=0`;
    return this.http.get<PaginatedCircularsResponse>(url);
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

  getCircularRecord(id: string): Observable<Circular> {
    return this.http.get<Circular>(
      `${this.baseUrl}/api/circulars/record/${id}`
    );
  }

  getActionItems(circularId: string): Observable<{ action_items: ActionItem[]; limit: number; offset: number; total: number }> {
    return this.http.get<{ action_items: ActionItem[]; limit: number; offset: number; total: number }>(
      `${this.baseUrl}/api/action-items?circular_id=${circularId}`
    );
  }

  getDepartments(circularId: string): Observable<{ departments: Department[] }> {
    return this.http.get<{ departments: Department[] }>(
      `${this.baseUrl}/api/circulars/${circularId}/departments`
    );
  }

  addDepartment(circularId: string, departmentId: string): Observable<{ departments: Department[] }> {
    return this.http.post<{ departments: Department[] }>(
      `${this.baseUrl}/api/circulars/${circularId}/departments`,
      { department_id: departmentId }
    );
  }

  removeDepartment(circularId: string, departmentId: string): Observable<{ departments: Department[] }> {
    return this.http.delete<{ departments: Department[] }>(
      `${this.baseUrl}/api/circulars/${circularId}/departments/${departmentId}`
    );
  }

  getAvailableDepartments(): Observable<{ items: Department[] }> {
    return this.http.get<{ items: Department[] }>(
      `${this.baseUrl}/api/properties/department`
    );
  }

  getSummary(recordId: string): Observable<{ summary: string }> {
    return this.http.get<{ summary: string }>(
      `${this.baseUrl}/api/circulars/${recordId}/summary`
    );
  }
}

interface ActionItem {
  id: string;
  action_item: string;
  circular_id: string;
  deadline: string;
  persona: string;
  priority: string;
  created_at: string;
  updated_at: string;
}

interface Department {
  id: string;
  name: string;
  type: string;
  archived?: boolean;
  metadata?: Record<string, unknown>;
  created_at: string;
  updated_at?: string;
}