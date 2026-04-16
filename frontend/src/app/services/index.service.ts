import { HttpClient, HttpParams } from '@angular/common/http';
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
  preview: string;
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
  full_reference: string;
  indexed_at: string;
  issue_date: string;
  pdf_url: string;
  source: string;
  title: string;
  url: string;
  file_path: string;
  archive_member_path?: string | null;
  asset_id?: string;
  asset_role?: string;
}

export interface CircularAsset {
  id: string;
  circular_id: string;
  asset_role: string;
  file_path: string;
  content_hash: string | null;
  mime_type: string | null;
  archive_member_path: string | null;
  file_size_bytes: number | null;
  created_at: string;
  updated_at: string;
}

export interface CircularDetail {
  id: string;
  source: string;
  circular_id: string;
  source_item_key: string;
  full_reference: string;
  department: string;
  title: string;
  issue_date: string;
  effective_date: string | null;
  url: string;
  pdf_url: string;
  status: string;
  file_path: string | null;
  content_hash: string | null;
  error_message: string | null;
  detected_at: string;
  created_at: string;
  updated_at: string;
  es_indexed_at: string | null;
  es_chunk_count: number | null;
  es_index_name: string | null;
}

export interface CircularDetailResponse {
  circular: CircularDetail;
  assets: CircularAsset[];
}

@Injectable({
  providedIn: 'root'
})

export class IndexService {

  constructor(private http: HttpClient) { }

  public getSearchResult(keyword: string, exchange: string[]): Observable<SearchIndexRespose> {

    let params = new HttpParams()
    .set('q', keyword)
    .set('exchange', exchange.join(','));


    return this.http.get<SearchIndexRespose>(`api/circulars/search`,
      { params }
    ).pipe(
      tap((x) => console.log('Search results:', x))
    );
  }

  public getCircularDetail(recordId: string): Observable<CircularDetailResponse> {
    return this.http.get<CircularDetailResponse>(`api/circulars/record/${recordId}`).pipe(
      tap((x) => console.log('Circular detail:', x))
    );
  }
}
