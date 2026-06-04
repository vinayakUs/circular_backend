import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class LoginService {
  private baseUrl = environment.apiUrl;

  constructor(private http: HttpClient) { }

  login(loginData: { username: string, password: string }):Observable<{access_token: string , token_type: string}> {
    return this.http.post<{access_token: string , token_type: string}>(`${this.baseUrl}/api/auth/login`, loginData);
  }

  isAuthenticated(): boolean {
    const cookies = document.cookie.split(";");
    for (const cookie of cookies) {
        const [name, value] = cookie.trim().split('=');
        if (name === 'access_token' && value) return true;
      }
    return false;
  }

  createProperty(name: string, type: string, metadata?: Record<string, unknown>): Observable<{id: string, name: string}> {
    return this.http.post<{id: string, name: string}>(`${this.baseUrl}/api/properties`, { name, type, metadata });
  }

  getProperties(type: string): Observable<{type: string, items: {id: string, name: string, archived: boolean}[]}> {
    return this.http.get<{type: string, items: {id: string, name: string, archived: boolean}[]}>(`${this.baseUrl}/api/properties/${type}`);
  }
}
