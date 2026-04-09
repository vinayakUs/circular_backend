import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';

export interface SearchIndexRespose {
  results: SearchIndexItem[];
  query: string;
}

export interface SearchIndexItem {
  id: string;
  score: number;
  document: CircularDocument;
}

export interface CircularDocument {
  chunk_id: string;
  chunk_index: number;
  chunk_text: string;
  circular_db_id: string;
  circular_id: string;
  content_hash: string;
  department: string;
  effective_date: string | null;
  file_path: string;
  full_reference: string;
  indexed_at: string;
  issue_date: string;
  pdf_url: string;
  source: string;
  title: string;
  url: string;
}

@Injectable({
  providedIn: 'root'
})

export class IndexService {

  constructor(private http: HttpClient) { }

  public getSearchResult(keyword: string): Observable<SearchIndexRespose> {
    return this.http.get<SearchIndexRespose>(`api/circulars/search`,
      { params: { q: keyword } }
    ).pipe(
      tap((x) => console.log('Search results:', x))
    );
  }
}
