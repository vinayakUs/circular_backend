import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../environments/environment';

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

@Injectable({ providedIn: 'root' })
export class PersonasApiService {
  constructor(private http: HttpClient) {}

  getPersonas(): Observable<Persona[]> {
    return this.http.get<Persona[]>(`${environment.apiUrl}/api/personas`);
  }
}