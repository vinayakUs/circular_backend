import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface CountsResponse {
  total: number;
  sebi: number;
  nse: number;
}

export interface Signatory {
  name: string;
  designation: string;
  extracted_at: string;
}

export interface Circular {
  id: string;
  source: string;
  circular_id: string;
  full_reference: string;
  department: string;
  title: string;
  issue_date: string;
  applicable_to_nse: boolean;
  status: string;
  url: string;
  signatories: Signatory[];
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
  references: string[];
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
  applicableToNse?: boolean;
}

export interface HighlightDetail {
  id: string;
  color: string;
  page: number;
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface Expert {
  id?: string;
  dept_id: string;
  dept_name?: string;
  title: string;
  text: string;
  highlights: any[];
}

export interface Signatory {
  name: string;
  designation: string;
  extracted_at: string;
}

export interface CircularDbLookupItem {
  id: string;
  matchedField: string;
  matchedValue: string;
}

export interface CircularDbLookupResponse {
  count: number;
  query: string;
  matches: CircularDbLookupItem[];
}

@Injectable({ providedIn: 'root' })
export class CircularsApiService {
  private http = inject(HttpClient);
  private baseUrl = environment.apiUrl;
  getCounts(): Observable<CountsResponse> {
    return this.http.get<CountsResponse>(
      `${this.baseUrl}/api/circulars/counts`
    );
  }

  getLatestCirculars(): Observable<PaginatedCircularsResponse> {
    const today: string = new Date().toISOString().split('T')[0];
    const url = `${this.baseUrl}/api/circulars?source=ALL&offset=0&from_date=${today}&to_date=${today}`;
    return this.http.get<PaginatedCircularsResponse>(url);
  }

  getlookupCirculars(params: { q: string; field?: string }): Observable<CircularDbLookupResponse> {
    const queryParams = new URLSearchParams();
    if (params.q) queryParams.set('q', params.q);
    if (params.field) queryParams.set('field', params.field);

    const url = `${this.baseUrl}/api/circulars/lookup?${queryParams.toString()}`;
    return this.http.get<CircularDbLookupResponse>(url);
  }

  getCirculars(params: {
    source?: string;
    limit?: number;
    offset?: number;
    from_date?: string;
    to_date?: string;
    search?: string;
    applicable_to_nse?: boolean | null;
    signatory?: string;
  }): Observable<PaginatedCircularsResponse> {
    const queryParams = new URLSearchParams();
    if (params.source) queryParams.set('source', params.source);
    if (params.limit) queryParams.set('limit', params.limit.toString());
    if (params.offset !== undefined) queryParams.set('offset', params.offset.toString());
    if (params.from_date) queryParams.set('from_date', params.from_date);
    if (params.to_date) queryParams.set('to_date', params.to_date);
    if (params.search) queryParams.set('search', params.search);
    if (params.applicable_to_nse !== null && params.applicable_to_nse !== undefined) {
      queryParams.set('applicable_to_nse', params.applicable_to_nse.toString());
    }
    if (params.signatory) queryParams.set('signatory', params.signatory);

    const url = `${this.baseUrl}/api/circulars?${queryParams.toString()}`;
    return this.http.get<PaginatedCircularsResponse>(url);
  }

  getCircularsv2(params: {
    source?: string;
    limit?: number;
    offset?: number;
    from_date?: string;
    to_date?: string;
    search?: string;
    applicable_to_nse?: boolean | null;
    signatory?: string[];
          circular_nos?: string[];
  }): Observable<PaginatedCircularsResponse> {
    const queryParams = new URLSearchParams();
    if (params.source) queryParams.set('source', params.source);
    if (params.limit) queryParams.set('limit', params.limit.toString());
    if (params.offset !== undefined) queryParams.set('offset', params.offset.toString());
    if (params.from_date) queryParams.set('from_date', params.from_date);
    if (params.to_date) queryParams.set('to_date', params.to_date);
    if (params.search) queryParams.set('search', params.search);
    if (params.applicable_to_nse !== null && params.applicable_to_nse !== undefined) {
      queryParams.set('applicable_to_nse', params.applicable_to_nse.toString());
    }
    if (params.signatory) {
      // queryParams.set('signatory', params.signatory)
      params.signatory?.forEach(signatory => queryParams.append('signatory', signatory));
    };
    if(params.circular_nos){
      params.circular_nos.forEach(n=>queryParams.append('circular_no',n));
    }

    const url = `${this.baseUrl}/api/circulars?${queryParams.toString()}`;
    return this.http.get<PaginatedCircularsResponse>(url);
  }
  semanticSearch(params: {
    query: string;
    strategy?: string;
    source?: string;
    from_date?: string;
    to_date?: string;
    applicable_to_nse?: boolean | null;
  }): Observable<SemanticSearchResponse> {
    return this.http.post<SemanticSearchResponse>(
      `${this.baseUrl}/api/circulars/search/hybrid`,
      { q: params.query, source: params.source, from_date: params.from_date, to_date: params.to_date, applicable_to_nse: params.applicable_to_nse }
    );
  }

  keywordSearch(params: {
    query: string;
    source?: string;
    from_date?: string;
    to_date?: string;
    applicable_to_nse?: boolean | null;
  }): Observable<SearchResponse> {
    return this.http.post<SearchResponse>(
      `${this.baseUrl}/api/circulars/search/bm25`,
      { q: params.query, source: params.source, from_date: params.from_date, to_date: params.to_date, applicable_to_nse: params.applicable_to_nse }
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

  saveExperts(circularId: string, experts: Expert[], originalIds: string[]): Observable<{ success: boolean }> {
    return this.http.post<{ success: boolean }>(
      `${this.baseUrl}/api/circulars/${circularId}/experts`,
      { experts, original_ids: originalIds }
    );
  }

  getExperts(circularId: string): Observable<{ experts: Expert[] }> {
    return this.http.get<{ experts: Expert[] }>(
      `${this.baseUrl}/api/circulars/${circularId}/experts`
    );
  }

  getSignatories(circularId: string): Observable<{ signatories: Signatory[] }> {
    return this.http.get<{ signatories: Signatory[] }>(
      `${this.baseUrl}/api/circulars/${circularId}/signatories`
    );
  }

  getAvailableSignatories(): Observable<{ items: { name: string }[] }> {
    return this.http.get<{ items: { name: string }[] }>(
      `${this.baseUrl}/api/signatories`
    );
  }

  getExpertsByDepartment(params: {
    department_id?: string;
    source?: string;
    from_date?: string;
    to_date?: string;
    full_circular_no?: string;
    page?: number;
    page_size?: number;
  }): Observable<ExpertsByDepartmentResponse> {
    const queryParams = new URLSearchParams();
    if (params.department_id) queryParams.set('department_id', params.department_id);
    if (params.source) queryParams.set('source', params.source);
    if (params.from_date) queryParams.set('from_date', params.from_date);
    if (params.to_date) queryParams.set('to_date', params.to_date);
    if (params.full_circular_no) queryParams.set('full_circular_no', params.full_circular_no);
    if (params.page) queryParams.set('page', String(params.page));
    if (params.page_size) queryParams.set('page_size', String(params.page_size));
    return this.http.get<ExpertsByDepartmentResponse>(
      `${this.baseUrl}/api/experts/by-department?${queryParams.toString()}`
    );
  }
}

export interface ExpertsByDepartmentResponse {
  experts: {
    items: ExpertWithCircular[];
    total: number;
    page: number;
    page_size: number;
    total_pages: number;
  };
  filters_applied: {
    department_id: string | null;
    source: string | null;
    from_date: string | null;
    to_date: string | null;
    full_circular_no: string | null;
  };
}

export interface ExpertWithCircular {
  id: string;
  expert_name: string;
  highlight_text: string;
  circular: {
    id: string;
    full_reference: string;
    source: string;
    issue_date: string;
    title: string;
  };
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

export interface Department {
  id: string;
  name: string;
  type: string;
  archived?: boolean;
  metadata?: Record<string, unknown>;
  created_at: string;
  updated_at?: string;
}