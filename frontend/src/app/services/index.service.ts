import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

export interface IndexSearchResult {
  id: string;
  title: string;
  source: 'NSE' | 'SEBI';
  date: string;
  circularId: string;
  referenceText: string;
}

@Injectable({
  providedIn: 'root'
})

export class IndexService {

  constructor(private http: HttpClient) { }

  public getSearchResult(keyword: string): Observable<IndexSearchResult[]> {
    return this.http.get<IndexSearchResult[]>(`api/circulars/search`,
      { params: { q: keyword } }
    );
  }
}
