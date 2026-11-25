import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface AiSummarySource {
  n: number;
  source: string;
  circular_id: string;
  circular_no: string;
  date: string;
  title: string;
  url: string;
}

export interface AiSearchResultItem {
  id: string;
  circular_id: string;
  circular_no: string;
  source: string;
  issue_date: string;
  title: string;
  url: string;
  department: string | null;
  short_summary: string | null;
}

export interface AiSearchPayload {
  plan: { understood_as: string; [k: string]: unknown };
  response: {
    understood_as: string;
    summary: { label: string; text: string; sources: AiSummarySource[] } | null;
    results: AiSearchResultItem[];
    facets: { source: Record<string, number>; year: Record<string, number> };
    message: string | null;
    [k: string]: unknown;
  };
  route: string;
  steps: number;
}

@Injectable({ providedIn: 'root' })
export class AiSearchApiService {
  private http = inject(HttpClient);
  private baseUrl = environment.apiUrl;

  search(q: string): Observable<AiSearchPayload> {
    return this.http.post<AiSearchPayload>(`${this.baseUrl}/api/search`, { q, summaries: true });
  }
}
